#include "CZhongTaiDispatcher.h"
#include "../../ini/CINIHandler.h"
#include "../../common/utility.h"
#include <rapidjson/document.h>
#include <filesystem>
#include <fstream>
#include <set>
#include <iterator>
#include <stdexcept>
using namespace trade;
using namespace trade::zhongtai;

#ifdef XBROKER_ENABLE_XTP
namespace
{
	std::string Text(const rapidjson::Value& value, const char* key)
	{
		if (!value.HasMember(key) || !value[key].IsString())
		{
			throw std::runtime_error(std::string("missing string: ") + key);
		}
		return { value[key].GetString(), value[key].GetStringLength() };
	}

	std::uint64_t Number(const rapidjson::Value& document, const char* key, std::uint64_t fallback, std::uint64_t minimum, std::uint64_t maximum)
	{
		if (!document.HasMember(key))
		{
			return fallback;
		}
		if (!document[key].IsUint64() || minimum > document[key].GetUint64() || maximum < document[key].GetUint64())
		{
			throw std::runtime_error(std::string("invalid option: ") + key);
		}
		return document[key].GetUint64();
	}
} // namespace
#endif

bool CZhongTaiDispatcher::IsInitialized() const
{
	return m_worker.joinable();
}

CResult CZhongTaiDispatcher::Initialize()
{
	if (nullptr != m_api || nullptr != m_store || IsInitialized())
	{
		return { Error::Conflict, "ZhongTai dispatcher already configured" };
	}
	std::string path = ini::CINIHandler::InstanceRef().GetValue(ini::Config::System, "Trade", "zhongtai_path", std::string());
	if (path.empty())
	{
		return {};
	}
#ifndef XBROKER_ENABLE_XTP
	return { Error::Unavailable, "zhongtai_path requires XBROKER_ENABLE_XTP" };
#else
	try
	{
		std::ifstream file(path);
		if (!file.is_open())
		{
			return { Error::Rejected, "cannot read ZhongTai configuration" };
		}
		std::string json((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
		rapidjson::Document document;
		document.Parse(json.data(), json.size());
		if (document.HasParseError() || !document.IsObject() || !document.HasMember("sites") || !document["sites"].IsArray())
		{
			return { Error::Rejected, "invalid ZhongTai configuration" };
		}
		CConfig config;
		config.m_software_key = Text(document, "software_key");
		config.m_software_version = Text(document, "software_version");
		config.m_log_path = Text(document, "log_path");
		if (!document.HasMember("client_id") || !document["client_id"].IsUint() || 1 > document["client_id"].GetUint() || 99 < document["client_id"].GetUint())
		{
			throw std::runtime_error("client_id must be 1..99");
		}
		config.m_client_id = document["client_id"].GetUint();
		config.m_queue_limit = static_cast<std::size_t>(Number(document, "queue_limit", 4096, 64, 65536));
		config.m_query_timeout_ms = static_cast<std::int64_t>(Number(document, "query_timeout_ms", 10000, 100, 10000));
		config.m_query_interval_ms = static_cast<std::int64_t>(Number(document, "query_interval_ms", 200, 20, 1000));
		config.m_refresh_ms = static_cast<std::int64_t>(Number(document, "refresh_ms", 5000, 1000, 60000));

		if (config.m_software_key.empty() || config.m_software_version.empty() || config.m_log_path.empty())
		{
			throw std::runtime_error("SDK key,version,log_path required");
		}
		std::set<std::pair<std::uint64_t, std::string>> names;
		for (const auto& value : document["sites"].GetArray())
		{
			if (!value.IsObject() || !value.HasMember("broker_id") || !value["broker_id"].IsUint64() || 0 == value["broker_id"].GetUint64() || !value.HasMember("port") || !value["port"].IsInt() || 0 >= value["port"].GetInt() || 65535 < value["port"].GetInt())
			{
				throw std::runtime_error("invalid ZhongTai site");
			}
			CSite site;
			site.m_broker_id = value["broker_id"].GetUint64();
			site.m_name = Text(value, "name");
			site.m_host = Text(value, "host");
			site.m_port = value["port"].GetInt();
			if (value.HasMember("local_ip"))
			{
				site.m_local_ip = Text(value, "local_ip");
			}
			if (site.m_name.empty() || site.m_host.empty() || !names.emplace(site.m_broker_id, site.m_name).second)
			{
				throw std::runtime_error("empty or duplicate ZhongTai site");
			}
			config.m_sites.emplace_back(std::move(site));
		}
		if (config.m_sites.empty())
		{
			throw std::runtime_error("at least one ZhongTai site required");
		}
		std::filesystem::create_directories(config.m_log_path);
		m_config = std::move(config);
		m_api = CreateZhongTaiApi(m_config);
		m_store = std::make_unique<CZhongTaiStore>(utility::MakeSaltHex());
		m_stopping = false;
		Start();
		return {};
	}
	catch (const std::exception& error)
	{
		Stop();
		m_api.reset();
		m_store.reset();
		return { Error::Unavailable, error.what() };
	}
#endif
}
