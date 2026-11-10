#include "CBrokerCenter.h"
#include "../service/CTradeService.h"
#include "../request/RequestCenter.h"
#include "../common/utility.h"
#include "../database/CDBEngine.h"
#include "../database/IDataBase.h"
#include <cctype>
#include "../network/CNetTools.h"
#include "../common/container.h"
#include <vector>

namespace
{
	constexpr int InvalidRequest = 1001;
	constexpr int InvalidCredentials = 1002;
	constexpr int StorageUnavailable = 1004;
	constexpr std::size_t MinUserLength = 3;
	constexpr std::size_t MaxUserLength = 64;
	constexpr std::size_t MinPasswordLength = 8;
	constexpr std::size_t MaxPasswordLength = 128;
	constexpr std::chrono::hours TokenLifetime{ 24 };

	bool IsUserValid(const std::string& strUser)
	{
		if ((MinUserLength > strUser.size()) || (MaxUserLength < strUser.size()))
		{
			return false;
		}
		for (unsigned char character : strUser)
		{
			if ((0 == std::isalnum(character)) && ('_' != character) && ('-' != character) && ('.' != character) && ('@' != character))
			{
				return false;
			}
		}
		return true;
	}

	bool IsPasswordValid(const std::string& strPassword)
	{
		return (MinPasswordLength <= strPassword.size()) && (MaxPasswordLength >= strPassword.size());
	}

} // namespace

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
	m_clientTokens.clear();
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

bool CBrokerCenter::HandleAuth(net::_TyConnectionId id, const CRequest& req)
{
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		m_clients.erase(id);
	}

	std::string strToken = req.GetExtraData("token");
	if (!strToken.empty())
	{
		return HandleReAuth(id, req);
	}

	if (_TyRequest::AUTH_LOGIN == req.GetType())
	{
		Login(id, req, strToken);
		return true;
	}
	net::SendError(id, req, InvalidRequest, "unsupported authentication request");
	return false;
}

bool CBrokerCenter::HandleReAuth(net::_TyConnectionId id, const CRequest& req)
{
	std::string strToken = req.GetExtraData("token");
	bool bAccepted = false;
	{
		std::lock_guard<std::mutex> lck(m_mtx_clients);
		const auto mIter = m_clientTokens.find(strToken);
		bAccepted = (m_clientTokens.end() != mIter) && (std::chrono::steady_clock::now() < mIter->second);
		if ((m_clientTokens.end() != mIter) && !bAccepted)
		{
			m_clientTokens.erase(mIter);
		}
		if (bAccepted)
		{
			m_clients.emplace(id);
		}
	}
	if (bAccepted)
	{
		SendAuthResponse(id, req, 0, "认证成功");
		return true;
	}
	SendAuthResponse(id, req, InvalidCredentials, "登录状态已失效");
	return false;
}

bool CBrokerCenter::Login(net::_TyConnectionId id, const CRequest& req, std::string& strToken)
{
	std::string strUser = req.GetExtraData("user");
	std::string strPassword = req.GetExtraData("password");
	if (!IsUserValid(strUser) || !IsPasswordValid(strPassword))
	{
		SendAuthResponse(id, req, InvalidCredentials, "账号或密码错误");
		return false;
	}

	db::_TyDBPtr db = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
	if (nullptr == db)
	{
		SendAuthResponse(id, req, StorageUnavailable, "用户数据库暂不可用");
		return false;
	}

	std::string strSql = "SELECT user_id, user FROM table_user WHERE user=" + utility::Utf8Literal(strUser) + " AND password_hash=UNHEX(SHA2(CONCAT(password_salt,UNHEX('" + utility::ToHex(strPassword) + "')),256)) AND status=1 LIMIT 1";
	const db::CTable& table = db->ExecQuery(strSql);
	if (table.m_rows.empty())
	{
		SendAuthResponse(id, req, InvalidCredentials, "账号或密码错误");
		return false;
	}

	CRequest response;
	response.SetId(req.GetId());
	response.SetType(req.GetType());
	response.SetErrorInfo(0, "");
	response.SetExtraData("user_id", db::to_string(table.m_rows.front().at(0)));
	response.SetExtraData("user", db::to_string(table.m_rows.front().at(1)));
	strToken = utility::MakeSaltHex();
	response.SetExtraData("token", strToken);
	{
		std::lock_guard<std::mutex> lock(m_mtx_clients);
		m_clients.emplace(id);
		m_clientTokens.insert_or_assign(strToken, std::chrono::steady_clock::now() + TokenLifetime);
	}
	net::SendRequest(id, response);
	return true;
}

void CBrokerCenter::SendAuthResponse(net::_TyConnectionId id, const CRequest& req, int nErrorCode, const std::string& strMessage) const
{
	CRequest response;
	response.SetId(req.GetId());
	response.SetType(req.GetType());
	if (0 != nErrorCode)
	{
		net::SetError(response, nErrorCode, strMessage);
	}
	else
	{

		response.SetErrorInfo(0, "");
	}
	net::SendRequest(id, response);
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
		HandleAuth(nConnection, request);
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
