#ifndef XBROKER_SYSTEM_CBOOTLOADER_H
#define XBROKER_SYSTEM_CBOOTLOADER_H

#include <filesystem>
#include <memory>
#include <string>

namespace net
{
	class CTcpServer;
}
class CSession;
class CTradeService;
class CBrokerCenter;

class CBootLoader final
{
public:
	CBootLoader();
	~CBootLoader();
	CBootLoader(const CBootLoader& arg) = delete;
	CBootLoader& operator=(const CBootLoader& arg) = delete;

	bool Initialize();
	bool Run();
	void Finalize();
	net::CTcpServer& GetTcpServer();
	CSession& GetSession();
	const std::string& GetLastError() const;
	int GetErrorCode() const;

private:
	bool Fail(int nErrorCode, const std::string& strMessage);
	std::filesystem::path m_exec;
	std::string m_last_error;
	int m_error_code{ 0 };
	bool m_initialized{ false };
	bool m_database_initialized{ false };
	std::unique_ptr<CTradeService> m_trade_service;
	std::unique_ptr<CBrokerCenter> m_center;
	std::unique_ptr<CSession> m_market;
	std::unique_ptr<net::CTcpServer> m_tcp_server;
};

#endif
