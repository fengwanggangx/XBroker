#include "CBootLoader.h"
#include "CSession.h"
#include "CBrokerCenter.h"
#include "../service/CTradeService.h"
#include "../database/CDBEngine.h"
#include "../database/IDataBase.h"
#include "../ini/CINIHandler.h"
#include "../network/CTcpServer.h"

CBootLoader::CBootLoader() = default;

CBootLoader::~CBootLoader()
{
	Finalize();
}

bool CBootLoader::Initialize()
{
	if (m_initialized)
	{
		return true;
	}
	m_error_code = 0;
	m_last_error.clear();
	m_exec = std::filesystem::current_path();

	net::EnvInitialize();
	if (!net::IsThreadEnable())
	{
		return Fail(1, "Failed to enable libevent thread support");
	}

	ini::CINIHandler& config = ini::CINIHandler::InstanceRef();
	int nPort = config.GetInt(ini::Config::System, "System", "tcp_port");
	if (0 >= nPort || 65535 < nPort)
	{
		return Fail(2, "XBroker database and valid port are required");
	}

	static const std::string cs_hqmarket = "HQMarket";
	CLoginInfo login;
	login.m_user = config.GetString(ini::Config::System, cs_hqmarket, "user");
	login.m_password = config.GetString(ini::Config::System, cs_hqmarket, "password");
	login.m_token = config.GetString(ini::Config::System, cs_hqmarket, "token");

	login.m_host.m_name = cs_hqmarket;
	login.m_host.m_host = config.GetString(ini::Config::System, cs_hqmarket, "host");
	login.m_host.m_port = config.GetInt(ini::Config::System, cs_hqmarket, "port");
	login.m_ordered = true;
	if (!login.Valid())
	{
		return Fail(3, "HQMarket endpoint and login credentials are required");
	}

	static const std::string cs_mysql = "MySQL";
	int nMySqlPort = config.GetInt(ini::Config::System, cs_mysql, "port");
	int nMySqlPoolSize = config.GetInt(ini::Config::System, cs_mysql, "pool_size");
	std::string strMySqlHost = config.GetString(ini::Config::System, cs_mysql, "host");
	std::string strMySqlUser = config.GetString(ini::Config::System, cs_mysql, "user");
	std::string strDatabase = config.GetString(ini::Config::System, cs_mysql, "database");
	if (strMySqlHost.empty() || strMySqlUser.empty() || (0 >= nMySqlPort) || (65535 < nMySqlPort) || (0 >= nMySqlPoolSize))
	{
		return Fail(4, "Invalid MySQL host, user, port or pool_size in ini/system.ini");
	}
	db::CConnectParam param(strMySqlHost, static_cast<unsigned int>(nMySqlPort), strMySqlUser, config.GetString(ini::Config::System, cs_mysql, "password"), strDatabase, "utf8mb4");
	m_database_initialized = true;
	if (0 != CDBEngine::InstanceRef().Initialize(db::em_database::mysql, param, nMySqlPoolSize))
	{
		return Fail(5, "XBroker MySQL initialization failed");
	}
	{
		db::_TyDBPtr database = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if ((nullptr == database) || (0 != database->ExecSqlFile(m_exec / "sql" / "table_create.sql")))
		{
			return Fail(6, "Failed to initialize XBroker MySQL tables");
		}
		if (0 != database->ExecSqlFile(m_exec / "sql" / "table_init.sql"))
		{
			return Fail(7, "Failed to execute sql/table_init.sql");
		}
	}

	m_market = std::make_unique<CSession>(login);
	m_tcp_server = std::make_unique<net::CTcpServer>(nPort);
	m_trade_service = std::make_unique<CTradeService>();
	m_center = std::make_unique<CBrokerCenter>(m_trade_service.get());
	m_center->SetMarketHandler(std::bind_front(&CSession::SendRequest, m_market.get()));
	if (!m_center->Initialize())
	{
		return Fail(8, m_center->GetLastError());
	}
	if (!m_trade_service->Initialize())
	{
		return Fail(9, m_trade_service->GetLastError());
	}
	m_tcp_server->RegisterHandler(std::bind_front(&CBrokerCenter::OnClientNetEvent, m_center.get()));
	m_market->RegisterHandler(std::bind_front(&CBrokerCenter::OnHQMarketReply, m_center.get()));
	m_market->RegisterStateHandler(std::bind_front(&CBrokerCenter::OnHQMarketState, m_center.get()));
	m_initialized = true;
	return true;
}

bool CBootLoader::Run()
{
	if (!m_initialized || nullptr == m_tcp_server || nullptr == m_market)
	{
		return Fail(10, "XBroker boot loader is not initialized");
	}
	if (0 != m_tcp_server->Initialize())
	{
		m_error_code = 11;
		m_last_error = "XBroker TCP server initialization failed";
		return false;
	}
	if (!m_market->Start())
	{
		m_error_code = 12;
		m_last_error = "HQMarket session initialization failed";
		return false;
	}
	m_tcp_server->Start(true);
	return true;
}

void CBootLoader::Finalize()
{
	if (nullptr != m_tcp_server)
	{
		m_tcp_server->ShutDown();
		m_tcp_server->Drain();
	}
	if (nullptr != m_market)
	{
		m_market->Stop();
		m_market->ClearHandlers();
	}
	if (nullptr != m_tcp_server)
	{
		m_tcp_server->ClearHandlers();
	}
	if (nullptr != m_trade_service)
	{
		m_trade_service->Stop();
	}
	if (nullptr != m_center)
	{
		m_center->Stop();
	}
	m_center.reset();
	m_trade_service.reset();
	m_tcp_server.reset();
	m_market.reset();
	if (m_database_initialized)
	{
		CDBEngine::InstanceRef().Close();
		m_database_initialized = false;
	}
	m_initialized = false;
}

bool CBootLoader::Fail(int nErrorCode, const std::string& strMessage)
{
	m_error_code = nErrorCode;
	m_last_error = strMessage;
	Finalize();
	return false;
}

const std::string& CBootLoader::GetLastError() const
{
	return m_last_error;
}

int CBootLoader::GetErrorCode() const
{
	return m_error_code;
}

net::CTcpServer& CBootLoader::GetTcpServer()
{
	return *m_tcp_server;
}

CSession& CBootLoader::GetSession()
{
	return *m_market;
}
