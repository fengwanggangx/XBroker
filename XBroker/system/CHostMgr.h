#ifndef XTRADER_CONFIGURATION_CSERVERSETTINGS_H
#define XTRADER_CONFIGURATION_CSERVERSETTINGS_H

#include "../common/ISingleton.h"

#include <optional>
#include <string>
#include <map>

struct CHostInfo
{
	std::string m_key;
	std::string m_name;
	std::string m_host;
	unsigned int m_port{ 0 };

	bool m_enabled{ false };

	bool operator==(const CHostInfo& arg) const;

	bool Valid() const;

	std::string Serialize() const;
	bool Deserialize(const std::string& key, const std::string& info);
};

class CHostMgr final
{
public:
	CHostMgr();

public:
	std::string Key() const;
	const std::map<std::string, CHostInfo>& GetHosts() const;
	std::optional<CHostInfo> GetActiveHost() const;

	bool SetConnectFast(bool enabled);
	bool IsConnectFast() const;

private:
	void Initialize();

private:
	bool m_connect_fast{ false };
	std::map<std::string, CHostInfo> m_hosts;
};

#endif
