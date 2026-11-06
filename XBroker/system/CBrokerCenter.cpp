#include "CBrokerCenter.h"
#include "../service/CTradeService.h"
#include "../request/RequestCenter.h"
#include "../ini/CINIHandler.h"
#include "../network/CNetTools.h"
#include "../common/container.h"
#include <vector>

CBrokerCenter::CBrokerCenter(CTradeService* pTradeService) : m_pTradeService(pTradeService)
{
}

CBrokerCenter::~CBrokerCenter()
{
	Stop();
}

void CBrokerCenter::Stop()
{
	// BootLoader drains network and counter callbacks before detaching the center.
	if (nullptr != m_pTradeService)
	{
		m_pTradeService->SetCounterReplyHandler({});
		m_pTradeService->SetCounterEventHandler({});
		m_pTradeService->SetPushHandler({});
		m_pTradeService->SetReplyHandler({});
		m_pTradeService->SetDepthHandler({});
	}
	m_handler_market = {};
	std::lock_guard<std::mutex> lock(m_mtx_clients);
	m_clients.clear();
	m_bInitialized = false;
}

bool CBrokerCenter::Initialize()
{
	if (m_bInitialized)
	{
		return true;
	}
	if (nullptr == m_pTradeService)
	{
		m_strLastError = "XBroker trade service is unavailable";
		return false;
	}
	m_strToken = ini::CINIHandler::InstanceRef().GetValue(ini::Config::System, "Broker", "token", std::string());
	if (m_strToken.empty())
	{
		m_strLastError = "XBroker service token is required";
		return false;
	}
	m_pTradeService->SetCounterReplyHandler(std::bind_front(&CBrokerCenter::OnCounterReply, this));
	m_pTradeService->SetCounterEventHandler(std::bind_front(&CBrokerCenter::OnCounterEvent, this));
	m_pTradeService->SetPushHandler(std::bind_front(&CBrokerCenter::Broadcast, this));
	m_pTradeService->SetReplyHandler(std::bind_front(&CBrokerCenter::OnReply, this));
	m_pTradeService->SetDepthHandler(std::bind_front(&CBrokerCenter::ChangeDepthSubscription, this));
	m_bInitialized = true;
	return true;
}

bool CBrokerCenter::OnReply(net::_TyConnectionId id, const CRequest& req)
{
	return net::SendRequest(id, req);
}

bool CBrokerCenter::OnHQMarketReply(const CRequest& request)
{
	return m_pTradeService->OnHQMarketReply(request);
}

void CBrokerCenter::OnHQMarketState(SessionState state, const std::string& message)
{
	m_pTradeService->OnHQMarketState(state, message);
}

void CBrokerCenter::SetMarketHandler(_TyRequestNotify&& handler)
{
	m_handler_market = std::move(handler);
}

bool CBrokerCenter::ChangeDepthSubscription(const std::string& security, bool subscribe)
{
	std::size_t dot = security.rfind('.');
	if (std::string::npos == dot || nullptr == m_handler_market)
	{
		return false;
	}
	CQuoteInfo quote(security.substr(0, dot), ParseMarket(security.substr(dot + 1)), Channel::depth);
	return m_handler_market(subscribe ? request::Subscription(quote) : request::UnSubscription(quote));
}

bool CBrokerCenter::Broadcast(const CRequest& req)
{
	std::vector<net::_TyConnectionId> clients;
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		clients.assign(m_clients.begin(), m_clients.end());
	}
	bool bSent = true;
	for (auto nId : clients)
	{
		bSent = net::SendRequest(nId, req) && bSent;
	}
	return bSent;
}

int CBrokerCenter::OnClientNetEvent(const net::CNetEvent& ev)
{
	net::_TyConnectionId nConnection = ev.m_connection_id;
	if (net::em_event::disconnected == ev.m_event || net::em_event::error == ev.m_event || net::em_event::timeout == ev.m_event)
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		m_clients.erase(nConnection);
		return 1;
	}
	if (net::em_event::request != ev.m_event || nullptr == ev.m_request)
	{
		return 0;
	}
	const CRequest& request = *ev.m_request;
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

	_TyRequest t = request.GetType();
	if (_TyRequest::HEARTBEAT == t)
	{
		CRequest response;
		response.SetType(request.GetType());
		response.SetId(request.GetId());
		net::SendRequest(nConnection, response);
		return 1;
	}
	switch (t)
	{
	case _TyRequest::SUPPORTED_BROKER_QUERY:
	case _TyRequest::TRADE_ACCOUNT_LOGIN:
	case _TyRequest::TRADE_ACCOUNT_LIST_QUERY:
	case _TyRequest::TRADE_ACCOUNT_DISCONNECT:
	case _TyRequest::TRADE_FUNDS_QUERY:
	case _TyRequest::TRADE_ORDER_SUBMIT:
	case _TyRequest::TRADE_ORDER_CANCEL:
	case _TyRequest::TRADE_POSITION_QUERY:
	case _TyRequest::TRADE_ORDER_QUERY:
	case _TyRequest::TRADE_DEAL_QUERY:
		m_pTradeService->OnRequest(request);
		break;
	default:
		net::SendError(nConnection, request, static_cast<int>(trade::Error::Unsupported), "Unsupported XBroker request");
		break;
	}
	return 1;
}

const std::string& CBrokerCenter::GetLastError() const
{
	return m_strLastError;
}

void CBrokerCenter::OnCounterReply(const trade::CCounterReply& reply)
{
	m_pTradeService->OnCounterReply(reply);
}

void CBrokerCenter::OnCounterEvent(const trade::CCountEvent& ev)
{
	m_pTradeService->OnCounterEvent(ev);
}
