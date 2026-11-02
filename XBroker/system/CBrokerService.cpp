#include "CBrokerService.h"
#include "../counter/zhongtai/CZhongTaiDispatcher.h"
#include "../counter/CSimulatedDispatcher.h"
#include "CSession.h"
#include "../request/request.h"
#include "../network/CTcpServer.h"
#include "../ini/CINIHandler.h"
#include "../network/CNetTools.h"
#include "../common/container.h"
#include <vector>
#include <functional>

CBrokerService::CBrokerService(net::CTcpServer* pTcpServer, CSession* pSession) : m_pTcpServer(pTcpServer), m_pSession(pSession)
{
}

CBrokerService::~CBrokerService()
{
	Stop();
}

void CBrokerService::Stop()
{
	if (m_bInitialized)
	{
		m_pTcpServer->ShutDown();
		m_pTcpServer->Drain();
		m_pTcpServer->ClearHandlers();
	}
	if (nullptr != m_dispatcher_simulated || nullptr != m_dispatcher_zhongtai)
	{
		// Quiesce HQMarket callbacks before stopping or destroying their targets.
		m_pSession->Stop();
		m_pSession->ClearHandlers();
	}
	m_trader.Stop();
	m_dispatcher_zhongtai.reset();
	m_dispatcher_simulated.reset();
	m_bInitialized = false;
}

bool CBrokerService::Initialize()
{
	if (m_bInitialized)
	{
		return true;
	}
	if (nullptr == m_pTcpServer || nullptr == m_pSession)
	{
		m_strLastError = "XBroker TCP server or HQMarket session is unavailable";
		return false;
	}
	m_strToken = ini::CINIHandler::InstanceRef().GetValue(ini::Config::System, "Broker", "token", std::string());
	if (m_strToken.empty())
	{
		m_strLastError = "XBroker service token is required";
		return false;
	}
	trade::CResult result = InitializeCounters();
	if (!result.Ok())
	{
		m_strLastError = result.m_strMessage;
		return false;
	}
	if (!m_trader.Initialize())
	{
		m_strLastError = m_trader.GetLastError();
		return false;
	}
	m_trader.SetPushHandler(std::bind_front(&CBrokerService::Broadcast, this));
	m_pTcpServer->RegisterHandler(std::bind_front(&CBrokerService::OnNetEvent, this));
	m_bInitialized = true;
	return true;
}

trade::CResult CBrokerService::InitializeCounters()
{
	CSession& session = *m_pSession;
	trade::CCounterFactory& creator = m_trader.CounterFactory();
	if (creator.Contains("simulated"))
	{
		return { trade::Error::Rejected, "Duplicate simulated counter registration" };
	}
	m_dispatcher_simulated = std::make_unique<CSimulatedDispatcher>();
	trade::CResult result = m_dispatcher_simulated->Initialize();
	if (!result.Ok())
	{
		return result;
	}
	session.RegisterHandler(std::bind_front(&CSimulatedDispatcher::OnMarketResponse, m_dispatcher_simulated.get()));
	m_dispatcher_simulated->SetDepthSubscriptionHandler(std::bind_front(&CSimulatedDispatcher::ChangeDepthSubscription, m_dispatcher_simulated.get(), std::ref(session)));
	session.RegisterStateHandler(std::bind_front(&CSimulatedDispatcher::OnSessionState, m_dispatcher_simulated.get()));
	m_dispatcher_simulated->OnSessionState(session.IsAuthenticated() ? SessionState::Ready : SessionState::Disconnected, "");
	if (!creator.Register("simulated", std::bind_front(&CSimulatedDispatcher::Create, m_dispatcher_simulated.get()), std::bind_front(&CSimulatedDispatcher::Stop, m_dispatcher_simulated.get())))
	{
		m_dispatcher_simulated->Stop();
		return { trade::Error::Rejected, "Duplicate simulated adapter registration" };
	}
	if (creator.Contains("zhongtai_xtp"))
	{
		return { trade::Error::Conflict, "Duplicate ZhongTai adapter registration" };
	}
	m_dispatcher_zhongtai = std::make_unique<CZhongTaiDispatcher>();
	result = m_dispatcher_zhongtai->Initialize();
	if (!result.Ok())
	{
		return result;
	}
	if (m_dispatcher_zhongtai->IsInitialized() && !creator.Register("zhongtai_xtp", std::bind_front(&CZhongTaiDispatcher::Create, m_dispatcher_zhongtai.get()), std::bind_front(&CZhongTaiDispatcher::Stop, m_dispatcher_zhongtai.get())))
	{
		m_dispatcher_zhongtai->Stop();
		return { trade::Error::Conflict, "Duplicate ZhongTai adapter registration" };
	}
	return {};
}

bool CBrokerService::Broadcast(const CRequest& push)
{
	std::vector<net::_TyConnectionId> clients;
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		clients.assign(m_clients.begin(), m_clients.end());
	}
	bool bSent = true;
	for (net::_TyConnectionId nId : clients)
	{
		bSent = net::SendRequest(nId, push) && bSent;
	}
	return bSent;
}

int CBrokerService::OnNetEvent(const net::CNetEvent& event)
{
	net::_TyConnectionId nConnection = event.m_connection_id;
	if (net::em_event::disconnected == event.m_event || net::em_event::error == event.m_event || net::em_event::timeout == event.m_event)
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		m_clients.erase(nConnection);
		return 1;
	}
	if (net::em_event::request != event.m_event || nullptr == event.m_request)
	{
		return 0;
	}
	const CRequest& request = *event.m_request;
	if (_TyRequest::AUTH_LOGIN == request.GetType())
	{
		bool bAuthorized = m_strToken == request.GetExtraData("token");
		{
			std::lock_guard<std::mutex> lock(m_mtx_clients);
			if (bAuthorized)
			{
				m_clients.emplace(nConnection);
			}
			else
			{
				m_clients.erase(nConnection);
			}
		}
		CRequest response;
		response.SetType(request.GetType());
		response.SetId(request.GetId());
		if (!bAuthorized)
		{
			response.SetErrorInfo(1, "Invalid XBroker service token");
		}
		else
		{
			response.SetExtraData("token", m_strToken);
		}
		net::SendRequest(nConnection, response);
		return 1;
	}
	bool bAuthorized = false;
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		bAuthorized = container::with(m_clients, nConnection);
	}
	if (!bAuthorized)
	{
		net::SendError(nConnection, request, 1, "XBroker authentication required");
		return 1;
	}
	if (_TyRequest::HEARTBEAT == request.GetType())
	{
		CRequest response;
		response.SetType(request.GetType());
		response.SetId(request.GetId());
		net::SendRequest(nConnection, response);
		return 1;
	}
	switch (request.GetType())
	{
	case _TyRequest::SUPPORTED_BROKER_QUERY:
	case _TyRequest::TRADE_ACCOUNT_LOGIN:
	case _TyRequest::TRADE_ACCOUNT_LIST_QUERY:
	case _TyRequest::TRADE_ACCOUNT_DISCONNECT:
	case _TyRequest::TRADE_FUNDS_QUERY:
		m_trader.HandleAccountRequest(request);
		break;
	case _TyRequest::TRADE_ORDER_SUBMIT:
	case _TyRequest::TRADE_ORDER_CANCEL:
	case _TyRequest::TRADE_POSITION_QUERY:
	case _TyRequest::TRADE_ORDER_QUERY:
	case _TyRequest::TRADE_DEAL_QUERY:
		m_trader.HandleTradeRequest(request);
		break;
	default:
		net::SendError(nConnection, request, static_cast<int>(trade::Error::Unsupported), "Unsupported XBroker request");
		break;
	}
	return 1;
}

const std::string& CBrokerService::GetLastError() const
{
	return m_strLastError;
}
