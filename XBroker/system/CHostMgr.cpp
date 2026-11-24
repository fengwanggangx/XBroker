#include "CHostMgr.h"

#include "../common/utility.h"
#include "../ini/CINIHandler.h"

#include <string_view>
#include <utility>
#include <vector>

namespace
{
	constexpr unsigned int portLowLimit = 1;
	constexpr unsigned int portUpLimit = 65535;
}

CHostMgr::CHostMgr()
{
	Initialize();
}

bool CHostInfo::Valid() const
{
	return !m_name.empty() && !m_host.empty() && (portLowLimit <= m_port) && (portUpLimit >= m_port);
}

bool CHostInfo::Deserialize(const std::string& key, const std::string& info)
{
	if (key.empty())
	{
		return false;
	}

	std::vector<std::string_view> values;
	if (4 != utility::split(info, values, '_', false))
	{
		return false;
	}

	unsigned int port = 0;
	if (!utility::to_number(std::string(values.at(2)), port) || (portLowLimit > port) || (portUpLimit < port))
	{
		return false;
	}

	m_key = key;
	m_name = values.at(0);
	m_host = values.at(1);
	m_port = port;
	m_enabled = "1" == values.at(3);
	return true;
}

bool CHostInfo::operator==(const CHostInfo& arg) const
{
	return (m_host == arg.m_host) && (m_port == arg.m_port);
}

std::string CHostInfo::Serialize() const
{
	if (!Valid())
	{
		return {};
	}
	return m_name + "_" + m_host + "_" + std::to_string(m_port) + "_" + (m_enabled ? "1" : "0");
}

void CHostMgr::Initialize()
{
	auto entries = ini::CINIHandler::InstanceRef().GetSection(ini::Config::System, "Server");
	m_hosts.clear();
	for (const auto& [k, v] : entries)
	{
		if ("connect_fast" == k)
		{
			m_connect_fast = "1" == v;
		}
		else if (0 == k.find("host"))
		{
			CHostInfo host;
			if (host.Deserialize(k, v))
			{
				m_hosts.emplace(k, std::move(host));
			}
		}
	}
}

std::string CHostMgr::Key() const
{
	return "host" + std::to_string(m_hosts.size());
}
const std::map<std::string, CHostInfo>& CHostMgr::GetHosts() const
{
	return m_hosts;
}

std::optional<CHostInfo> CHostMgr::GetActiveHost() const
{
	for (const auto& v : m_hosts)
	{
		const CHostInfo& info = v.second;
		if (info.m_enabled && info.Valid())
		{
			return info;
		}
	}
	return std::nullopt;
}

bool CHostMgr::SetConnectFast(bool enabled)
{
	bool previous = m_connect_fast;
	m_connect_fast = enabled;
	return previous;
}

bool CHostMgr::IsConnectFast() const
{
	return m_connect_fast;
}
