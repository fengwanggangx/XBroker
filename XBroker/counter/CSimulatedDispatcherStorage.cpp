#include "CSimulatedDispatcher.h"
#include "CStorager.h"
#include "../common/utility.h"
#include "../ini/CINIHandler.h"
#include <rapidjson/document.h>
#include <fstream>
#include <iterator>
#include <chrono>

namespace
{
	std::int64_t SystemTime()
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	}

	bool Integer(const rapidjson::Value& value, const char* pName, std::int64_t& nValue)
	{
		if (!value.HasMember(pName) || !value[pName].IsInt64())
		{
			return false;
		}
		nValue = value[pName].GetInt64();
		return true;
	}

	bool Text(const rapidjson::Value& value, const char* pName, std::string& strValue)
	{
		if (!value.HasMember(pName) || !value[pName].IsString())
		{
			return false;
		}
		strValue.assign(value[pName].GetString(), value[pName].GetStringLength());
		return true;
	}

	trade::CResult ReadRules(const std::string& strPath, std::vector<trade::CRule>& rules, std::vector<trade::CCalendar>& calendar)
	{
		if (strPath.empty())
		{
			return {};
		} // Safe default: account queries work, all orders lack rules and are rejected.
		std::ifstream stream(strPath);
		if (!stream.is_open())
		{
			return { trade::Error::Rejected, "cannot open Trade.rules_path" };
		}
		std::string strJson((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
		rapidjson::Document document;
		document.Parse(strJson.data(), strJson.size());
		if (document.HasParseError() || !document.IsObject() || !document.HasMember("rules") || !document["rules"].IsArray() || !document.HasMember("calendar") || !document["calendar"].IsArray())
		{
			return { trade::Error::Rejected, "invalid counter rules JSON" };
		}
		for (const auto& value : document["calendar"].GetArray())
		{
			trade::CCalendar day;
			std::int64_t nDay = 0;
			if (!value.IsObject() || !Integer(value, "day", nDay) || (0 >= nDay) || (99991231 < nDay) || !Integer(value, "open_ms", day.m_nOpen) || !Integer(value, "break_ms", day.m_nBreak) || !Integer(value, "resume_ms", day.m_nResume) || !Integer(value, "close_ms", day.m_nClose))
			{
				return { trade::Error::Rejected, "calendar requires explicit day and epoch-millisecond sessions" };
			}
			day.m_nDay = static_cast<int>(nDay);
			calendar.emplace_back(day);
		}
		for (const auto& value : document["rules"].GetArray())
		{
			trade::CRule rule;
			std::int64_t nDay = 0, nScale = 0;
			if (!value.IsObject() || !Text(value, "version", rule.m_strVersion) || !Text(value, "security", rule.m_strSecurity) || !Text(value, "board", rule.m_strBoard) || !Integer(value, "day", nDay) || (0 >= nDay) || (99991231 < nDay) || !Integer(value, "price_scale", nScale) || (0 > nScale) || (6 < nScale) || !Integer(value, "tick", rule.m_nTick) || !Integer(value, "buy_minimum", rule.m_nBuyMinimum) || !Integer(value, "buy_step", rule.m_nBuyStep) || !Integer(value, "sell_minimum", rule.m_nSellMinimum) || !Integer(value, "sell_step", rule.m_nSellStep) || !Integer(value, "lower", rule.m_nLower) || !Integer(value, "upper", rule.m_nUpper) || !Integer(value, "commission_ppm", rule.m_nCommissionPpm) || !Integer(value, "minimum_fee_cents", rule.m_nMinimumFee) || !Integer(value, "sell_tax_ppm", rule.m_nSellTaxPpm) || !Integer(value, "transfer_ppm", rule.m_nTransferPpm) || !value.HasMember("corporate_action") || !value["corporate_action"].IsBool())
			{
				return { trade::Error::Rejected, "security rule fields must be explicit" };
			}
			rule.m_nDay = static_cast<int>(nDay);
			rule.m_nPriceScale = static_cast<int>(nScale);
			rule.m_bCorporateAction = value["corporate_action"].GetBool();
			rules.emplace_back(std::move(rule));
		}
		return {};
	}
} // namespace

trade::CResult CSimulatedDispatcher::Initialize()
{
	try
	{
		std::string strRulesPath = ini::CINIHandler::InstanceRef().GetValue(ini::Config::System, "Trade", "rules_path", std::string());
		std::vector<trade::CRule> rules;
		std::vector<trade::CCalendar> calendar;
		trade::CResult result = ReadRules(strRulesPath, rules, calendar);
		if (!result.Ok())
		{
			return result;
		}
		return Start({}, rules, calendar);
	}
	catch (const std::exception& error)
	{
		Stop();
		return { trade::Error::Unavailable, error.what() };
	}
}

trade::CResult CSimulatedDispatcher::Start(const std::vector<trade::CFunds>& accounts, const std::vector<trade::CRule>& rules, const std::vector<trade::CCalendar>& calendar)
{
	return Start(std::make_unique<trade::CStorager>(utility::MakeSaltHex()), SystemTime, accounts, rules, calendar);
}
