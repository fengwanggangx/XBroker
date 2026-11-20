#include "CSimulatedDispatcher.h"
#include "CStorager.h"
#include "../common/utility.h"
#include "../ini/CINIHandler.h"
#include "../database/CDBEngine.h"
#include "../database/IDataBase.h"
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
			if (!value.IsObject() || !Integer(value, "day", nDay) || (0 >= nDay) || (99991231 < nDay) || !Integer(value, "open_ms", day.m_open_time_ms) || !Integer(value, "break_ms", day.m_break_time_ms) || !Integer(value, "resume_ms", day.m_resume_time_ms) || !Integer(value, "close_ms", day.m_close_time_ms))
			{
				return { trade::Error::Rejected, "calendar requires explicit day and epoch-millisecond sessions" };
			}
			day.m_day = static_cast<int>(nDay);
			calendar.emplace_back(day);
		}
		for (const auto& value : document["rules"].GetArray())
		{
			trade::CRule rule;
			std::int64_t nDay = 0, nScale = 0;
			if (!value.IsObject() || !Text(value, "version", rule.m_version_text) || !Text(value, "security", rule.m_security) || !Text(value, "board", rule.m_board) || !Integer(value, "day", nDay) || (0 >= nDay) || (99991231 < nDay) || !Integer(value, "price_scale", nScale) || (0 > nScale) || (6 < nScale) || !Integer(value, "tick", rule.m_tick) || !Integer(value, "buy_minimum", rule.m_buy_minimum) || !Integer(value, "buy_step", rule.m_buy_step) || !Integer(value, "sell_minimum", rule.m_sell_minimum) || !Integer(value, "sell_step", rule.m_sell_step) || !Integer(value, "lower", rule.m_lower) || !Integer(value, "upper", rule.m_upper) || !Integer(value, "commission_ppm", rule.m_commission_ppm) || !Integer(value, "minimum_fee_cents", rule.m_minimum_fee) || !Integer(value, "sell_tax_ppm", rule.m_sell_tax_ppm) || !Integer(value, "transfer_ppm", rule.m_transfer_ppm) || !value.HasMember("corporate_action") || !value["corporate_action"].IsBool())
			{
				return { trade::Error::Rejected, "security rule fields must be explicit" };
			}
			rule.m_day = static_cast<int>(nDay);
			rule.m_price_scale = static_cast<int>(nScale);
			rule.m_corporate_action = value["corporate_action"].GetBool();
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
		db::_TyDBPtr pDB = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr == pDB)
		{
			return { trade::Error::Unavailable, "simulated account database unavailable" };
		}
		const db::CTable& table = pDB->ExecQuery("SELECT broker_id, account, initial_balance_cents FROM table_account WHERE status=1");
		if (table.m_columns.empty())
		{
			return { trade::Error::Unavailable, "cannot load simulated accounts" };
		}
		std::vector<trade::CFunds> accounts;
		accounts.reserve(table.m_rows.size());
		for (const db::_TyRow& row : table.m_rows)
		{
			trade::CFunds funds;
			if ((3 != row.size()) ||
				!utility::to_number(db::to_string(row[0]), funds.m_account_index.first) ||
				!utility::to_number(db::to_string(row[2]), funds.m_balance))
			{
				return { trade::Error::Rejected, "invalid simulated account" };
			}
			funds.m_account_index.second = db::to_string(row[1]);
			accounts.emplace_back(std::move(funds));
		}
		return Start(accounts, rules, calendar);
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
