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
	if (m_bInitialized)
	{
		return true;
	}
	m_nErrorCode = 0;
	m_strLastError.clear();
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
	login.m_strUser = config.GetString(ini::Config::System, cs_hqmarket, "user");
	login.m_strPassword = config.GetString(ini::Config::System, cs_hqmarket, "password");
	login.m_strToken = config.GetString(ini::Config::System, cs_hqmarket, "token");

	login.m_host.m_strName = cs_hqmarket;
	login.m_host.m_strHost = config.GetString(ini::Config::System, cs_hqmarket, "host");
	login.m_host.m_nPort = config.GetInt(ini::Config::System, cs_hqmarket, "port");
	login.m_bOrdered = true;
	if (!login.Valid())
	{
		return Fail(5, "HQMarket endpoint and login credentials are required");
	}

	static const std::string cs_mysql = "MySQL";
	int nMySqlPort = config.GetInt(ini::Config::System, cs_mysql, "port");
	int nMySqlPoolSize = config.GetInt(ini::Config::System, cs_mysql, "pool_size");
	std::string strMySqlHost = config.GetString(ini::Config::System, cs_mysql, "host");
	std::string strMySqlUser = config.GetString(ini::Config::System, cs_mysql, "user");
	std::string strDatabase = config.GetString(ini::Config::System, cs_mysql, "database");
	if (strMySqlHost.empty() || strMySqlUser.empty() || (0 >= nMySqlPort) || (65535 < nMySqlPort) || (0 >= nMySqlPoolSize))
	{
		return Fail(3, "Invalid MySQL host, user, port or pool_size in ini/system.ini");
	}
	db::CConnectParam param(strMySqlHost, static_cast<unsigned int>(nMySqlPort), strMySqlUser, config.GetString(ini::Config::System, cs_mysql, "password"), strDatabase, "utf8mb4");
	m_bDatabaseInitialized = true;
	if (0 != CDBEngine::InstanceRef().Initialize(db::em_database::mysql, param, nMySqlPoolSize))
	{
		return Fail(3, "XBroker MySQL initialization failed");
	}
	bool bTablesReady = false;
	{
		db::_TyDBPtr database = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		bTablesReady = nullptr != database && 0 == database->ExecSqlFile(m_exec / "sql" / "table_create.sql");
	}
	if (!bTablesReady)
	{
		return Fail(4, "Failed to initialize XBroker MySQL tables");
	}

	m_pMarket = std::make_unique<CSession>(login);
	m_pTcpServer = std::make_unique<net::CTcpServer>(nPort);
	m_pTradeService = std::make_unique<CTradeService>();
	m_pCenter = std::make_unique<CBrokerCenter>(m_pTradeService.get());
	m_pCenter->SetMarketHandler(std::bind_front(&CSession::SendRequest, m_pMarket.get()));
	if (!m_pCenter->Initialize())
	{
		return Fail(7, m_pCenter->GetLastError());
	}
	if (!m_pTradeService->Initialize())
	{
		return Fail(7, m_pTradeService->GetLastError());
	}
	m_pTcpServer->RegisterHandler(std::bind_front(&CBrokerCenter::OnClientNetEvent, m_pCenter.get()));
	m_pMarket->RegisterHandler(std::bind_front(&CBrokerCenter::OnHQMarketReply, m_pCenter.get()));
	m_pMarket->RegisterStateHandler(std::bind_front(&CBrokerCenter::OnHQMarketState, m_pCenter.get()));
	m_bInitialized = true;
	return true;
}

bool CBootLoader::Run()
{
	if (!m_bInitialized || nullptr == m_pTcpServer || nullptr == m_pMarket)
	{
		return Fail(6, "XBroker boot loader is not initialized");
	}
	if (0 != m_pTcpServer->Initialize())
	{
		m_nErrorCode = 6;
		m_strLastError = "XBroker TCP server initialization failed";
		return false;
	}
	if (!m_pMarket->Start())
	{
		m_nErrorCode = 5;
		m_strLastError = "HQMarket session initialization failed";
		return false;
	}
	m_pTcpServer->Start(true);
	return true;
}

void CBootLoader::Finalize()
{
	if (nullptr != m_pTcpServer)
	{
		m_pTcpServer->ShutDown();
		m_pTcpServer->Drain();
	}
	if (nullptr != m_pMarket)
	{
		m_pMarket->Stop();
		m_pMarket->ClearHandlers();
	}
	if (nullptr != m_pTcpServer)
	{
		m_pTcpServer->ClearHandlers();
	}
	if (nullptr != m_pTradeService)
	{
		m_pTradeService->Stop();
	}
	if (nullptr != m_pCenter)
	{
		m_pCenter->Stop();
	}
	m_pCenter.reset();
	m_pTradeService.reset();
	m_pTcpServer.reset();
	m_pMarket.reset();
	if (m_bDatabaseInitialized)
	{
		CDBEngine::InstanceRef().Close();
		m_bDatabaseInitialized = false;
	}
	m_bInitialized = false;
}

bool CBootLoader::Fail(int nErrorCode, const std::string& strMessage)
{
	m_nErrorCode = nErrorCode;
	m_strLastError = strMessage;
	Finalize();
	return false;
}

const std::string& CBootLoader::GetLastError() const
{
	return m_strLastError;
}

int CBootLoader::GetErrorCode() const
{
	return m_nErrorCode;
}

net::CTcpServer& CBootLoader::GetTcpServer()
{
	return *m_pTcpServer;
}

CSession& CBootLoader::GetSession()
{
	return *m_pMarket;
}
