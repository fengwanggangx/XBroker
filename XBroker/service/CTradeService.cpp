#include "CTradeService.h"
#include "../counter/TradeProtocol.h"
#include "../common/utility.h"
#include "../common/container.h"
#include "../ini/CINIHandler.h"
#include "../network/CNetTools.h"
#include "../request/request.pb.h"
#include <rapidjson/document.h>
#include <fstream>
#include <iterator>
#include <charconv>

namespace
{
	bool ReadId(const std::string& text, std::uint64_t& value)
	{
		std::from_chars_result result = std::from_chars(text.data(), text.data() + text.size(), value);
		return !text.empty() && std::errc() == result.ec && text.data() + text.size() == result.ptr && 0 != value;
	}

	std::uint64_t SourceId(const CRequest& request)
	{
		std::uint64_t source = request.GetId();
		utility::to_number(request.GetExtraData("source_request_id"), source);
		return source;
	}

	CRequest Response(const CRequest& request)
	{
		CRequest response;
		response.SetId(request.GetId());
		response.SetType(request.GetType());
		return response;
	}
} // namespace

CTradeService::CTradeService()
{
	m_handler_request = {
		{ _TyRequest::SUPPORTED_BROKER_QUERY, std::bind_front(&CTradeService::HandleQueryBrokerSupported, this) },
		{ _TyRequest::TRADE_ACCOUNT_LOGIN, std::bind_front(&CTradeService::HandleAccountLogin, this) },
		{ _TyRequest::TRADE_ACCOUNT_LIST_QUERY, std::bind_front(&CTradeService::HandleAccountListQuery, this) },
		{ _TyRequest::TRADE_ACCOUNT_DISCONNECT, std::bind_front(&CTradeService::HandleAccountDisconnect, this) },
		{ _TyRequest::TRADE_ORDER_SUBMIT, std::bind_front(&CTradeService::HandleOrderSubmit, this) },
		{ _TyRequest::TRADE_ORDER_CANCEL, std::bind_front(&CTradeService::HandleOrderCancel, this) },
		{ _TyRequest::TRADE_FUNDS_QUERY, std::bind_front(&CTradeService::HandleFundsQuery, this) },
		{ _TyRequest::TRADE_POSITION_QUERY, std::bind_front(&CTradeService::HandlePositionQuery, this) },
		{ _TyRequest::TRADE_ORDER_QUERY, std::bind_front(&CTradeService::HandleOrderQuery, this) },
		{ _TyRequest::TRADE_DEAL_QUERY, std::bind_front(&CTradeService::HandleDealQuery, this) },
	};
}

CTradeService::~CTradeService()
{
	Stop();
}

bool CTradeService::LoadBrokers()
{
	std::string path = ini::CINIHandler::InstanceRef().GetValue(ini::Config::System, "Trade", "brokers_path", std::string("ini/brokers.json"));
	std::ifstream stream(path);
	std::string strText((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());

	rapidjson::Document document;
	document.Parse(strText.data(), strText.size());
	if (document.HasParseError() || !document.IsArray())
	{
		m_strLastError = "Invalid broker directory";
		return false;
	}

	for (const auto& value : document.GetArray())
	{
		if (!value.IsObject() || !value.HasMember("id") || !value["id"].IsUint64() || 0 == value["id"].GetUint64() || !value.HasMember("name") || !value["name"].IsString() || !value.HasMember("adapter") || !value["adapter"].IsString() || !value.HasMember("sites") || !value["sites"].IsArray())
		{
			m_strLastError = "Invalid broker configuration";
			return false;
		}
		CBrokerInfo broker;
		broker.m_brokerId = value["id"].GetUint64();
		broker.m_strName = value["name"].GetString();
		broker.m_strAdapter = value["adapter"].GetString();
		for (const auto& site : value["sites"].GetArray())
		{
			if (!site.IsString() || 0 == site.GetStringLength() || !broker.m_sites.emplace(site.GetString()).second)
			{
				m_strLastError = "Invalid or duplicate broker site";
				return false;
			}
		}
		if (broker.m_sites.empty() || broker.m_strName.empty() || !m_brokers.emplace(value["id"].GetUint64(), std::move(broker)).second)
		{
			m_strLastError = "Empty broker name/sites or duplicate broker id";
			return false;
		}
	}
	for (const auto& item : m_brokers)
	{
		if (!m_counterFactory.Contains(item.second.m_strAdapter))
		{
			m_strLastError = "Unknown broker adapter";
			return false;
		}
	}
	return true;
}

bool CTradeService::Initialize()
{
	return LoadBrokers();
}

void CTradeService::Stop()
{
	std::vector<std::shared_ptr<trade::ITradeSession>> counters;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		m_bStopping = true;
		counters.swap(m_retired);
		for (auto& item : m_accounts)
		{
			counters.emplace_back(item.second.m_session);
			item.second.m_info.m_state = TradeAccountState::Disconnected;
		}
		for (const auto& item : m_logging)
		{
			counters.emplace_back(item.second);
		}
	}
	for (const auto& counter : counters)
	{
		if (nullptr != counter)
		{
			counter->Close();
		}
	}
	m_counterFactory.Stop();
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		m_accounts.clear();
		m_logging.clear();
		m_retired.clear();
	}
	m_handler_push = {};
}

const std::string& CTradeService::GetLastError() const
{
	return m_strLastError;
}

bool CTradeService::HasAccount(const _TyAccount& account) const
{
	std::lock_guard<std::mutex> lock(m_mtx_accounts);
	return container::with(m_accounts, account);
}

bool CTradeService::HandleAccountRequest(const CRequest& req)
{
	return DispatchRequest(req);
}

bool CTradeService::HandleTradeRequest(const CRequest& req)
{
	return DispatchRequest(req);
}

bool CTradeService::DispatchRequest(const CRequest& req)
{
	std::vector<std::shared_ptr<trade::ITradeSession>> counters;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		counters.swap(m_retired);
	}
	for (const auto& counter : counters)
	{
		counter->Close();
	}
	const auto mIter = m_handler_request.find(req.GetType());
	if (m_handler_request.end() == mIter)
	{
		net::SendError(req.GetConnectionId(), req, 3, "Unsupported trading request");
		return false;
	}
	return mIter->second(req);
}

bool CTradeService::HandleQueryBrokerSupported(const CRequest& req)
{
	CRequest response = Response(req);
	request::BrokerList list;
	for (const auto& item : m_brokers)
	{
		request::BrokerRecord* broker = list.add_brokers();
		broker->set_broker_id(item.second.m_brokerId);
		broker->set_name(item.second.m_strName);
		for (const auto& site : item.second.m_sites)
		{
			request::BrokerSite* target = broker->add_sites();
			target->set_site(site);
			target->set_name(site);
		}
	}
	response.SetData(list);
	return net::SendRequest(req.GetConnectionId(), response);
}

bool CTradeService::HandleAccountLogin(const CRequest& req)
{
	_TyAccount account;
	if (!ReadAccountIdentity(req, account) || req.GetExtraData("password").empty())
	{
		net::SendError(req.GetConnectionId(), req, 1202, "broker_id/site/account/password required");
		return false;
	}
	const auto broker = m_brokers.find(account.first);
	if (m_brokers.end() == broker || !container::with(broker->second.m_sites, req.GetExtraData("site")))
	{
		net::SendError(req.GetConnectionId(), req, 3, "Unknown broker or site");
		return false;
	}
	bool bLoginBlocked = false;
	bool bConnected = false;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		bLoginBlocked = m_bStopping || container::with(m_logging, account);
		if (!bLoginBlocked)
		{
			const auto mIter = m_accounts.find(account);
			bConnected = m_accounts.end() != mIter && TradeAccountState::Connected == mIter->second.m_info.m_state;
			if (m_accounts.end() != mIter && (TradeAccountState::Reconnecting == mIter->second.m_info.m_state || TradeAccountState::Synchronizing == mIter->second.m_info.m_state))
			{
				bLoginBlocked = true;
			}
			if (!bConnected && !bLoginBlocked)
			{
				if (m_accounts.end() != mIter)
				{
					mIter->second.m_info.m_state = TradeAccountState::Connecting;
					mIter->second.m_info.m_strLastError.clear();
				}
				m_logging.emplace(account, nullptr);
			}
		}
	}
	if (bLoginBlocked)
	{
		net::SendError(req.GetConnectionId(), req, 6, "Login in progress or service stopping");
		return false;
	}
	if (bConnected)
	{
		CRequest response = Response(req);
		trade::CCounterReply reply;
		reply.m_accountIndex = account;
		reply.m_operation = trade::Operation::Login;
		reply.m_data = trade::COperationReceipt{};
		trade::protocol::WriteReply(reply, response);
		return net::SendRequest(req.GetConnectionId(), response);
	}
	std::shared_ptr<trade::ITradeSession> counter = m_counterFactory.Create({ account, broker->second.m_strAdapter });
	if (nullptr == counter)
	{
		{
			std::lock_guard<std::mutex> lock(m_mtx_accounts);
			m_logging.erase(account);
		}
		net::SendError(req.GetConnectionId(), req, 3, "Adapter unavailable");
		return false;
	}
	{
		std::weak_ptr<trade::ITradeSession> weak = counter;
		counter->SetEventHandler([this, account, weak](const trade::CCountEvent& event)
		{
			{
				std::lock_guard<std::mutex> lock(m_mtx_accounts);
				const auto current = m_accounts.find(account);
				if (m_accounts.end() == current || current->second.m_session != weak.lock() || account != event.m_accountIndex)
				{
					return;
				}
			}
			OnEvent(event);
		});
	}
	bool bStopping = false;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		bStopping = m_bStopping;
		if (bStopping)
		{
			m_logging.erase(account);
		}
		else
		{
			m_logging[account] = counter;
		}
	}
	if (bStopping)
	{
		counter->Close();
		net::SendError(req.GetConnectionId(), req, 2, "Service stopping");
		return false;
	}
	trade::CReqParam login;
	login.m_accountIndex = account;
	login.m_requestId = SourceId(req);
	login.m_param = trade::CLoginParam{ req.GetExtraData("password"), req.GetExtraData("site") };
	net::_TyConnectionId connection = req.GetConnectionId();
	std::uint64_t requestId = req.GetId();
	std::string brokerName = broker->second.m_strName;
	trade::CResult queued = counter->SendRequest(std::move(login), [this, account, connection, requestId, brokerName, site = req.GetExtraData("site")](const trade::CCounterReply& reply)
	{
		CRequest response;
		response.SetId(requestId);
		response.SetType(_TyRequest::TRADE_ACCOUNT_LOGIN);
		trade::CResult result = reply.m_result;
		if (result.Ok() && (account != reply.m_accountIndex || trade::Operation::Login != reply.m_operation))
		{
			result = { trade::Error::Unavailable, "Invalid adapter login response" };
		}
		CTradeAccountInfo info = AccountInfo(account);
		info.m_strBrokerName = brokerName;
		info.m_strSite = site;
		info.m_strName = info.m_strAccount;
		info.m_state = TradeAccountState::Connected;
		{
			std::lock_guard<std::mutex> lock(m_mtx_accounts);
			const auto pending = m_logging.find(account);
			std::shared_ptr<trade::ITradeSession> counter = m_logging.end() == pending ? nullptr : pending->second;
			m_logging.erase(account);
			if (m_bStopping)
			{
				result = { trade::Error::Unavailable, "Service stopping" };
			}
			if (nullptr != counter && !m_bStopping && result.Ok())
			{
				const auto old = m_accounts.find(account);
				if (m_accounts.end() != old && old->second.m_session != counter)
				{
					m_retired.emplace_back(old->second.m_session);
				}
				m_accounts.insert_or_assign(account, CTradeAccount{ info, counter });
			}
			else if (nullptr != counter)
			{
				m_retired.emplace_back(std::move(counter));
			}
			if (!result.Ok())
			{
				const auto existing = m_accounts.find(account);
				if (m_accounts.end() != existing)
				{
					existing->second.m_info.m_state = TradeAccountState::Error;
					existing->second.m_info.m_strLastError = result.m_strMessage;
				}
			}
		}
		response.SetErrorInfo(static_cast<int>(result.m_error), result.m_strMessage);
		if (result.Ok())
		{
			trade::protocol::WriteReply(reply, response);
		}
		net::SendRequest(connection, response);
	});
	if (!queued.Ok())
	{
		{
			std::lock_guard<std::mutex> lock(m_mtx_accounts);
			m_logging.erase(account);
			m_retired.emplace_back(counter);
		}
		net::SendError(connection, req, static_cast<int>(queued.m_error), queued.m_strMessage);
	}
	return queued.Ok();
}

bool CTradeService::HandleAccountListQuery(const CRequest& req)
{
	CRequest response = Response(req);
	request::TradeAccountList list;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		for (const auto& item : m_accounts)
		{
			*list.add_accounts() = trade::protocol::AccountPayload(item.second.m_info);
		}
	}
	response.SetData(list);
	return net::SendRequest(req.GetConnectionId(), response);
}

bool CTradeService::HandleFundsQuery(const CRequest& req)
{
	trade::CReqParam param;
	param.m_param = trade::CFundsQueryParam{};
	return SendRequest(req, std::move(param));
}

bool CTradeService::HandleAccountDisconnect(const CRequest& req)
{
	trade::CReqParam param;
	param.m_param = trade::CDisconnectParam{};
	return SendRequest(req, std::move(param));
}

bool CTradeService::HandleOrderSubmit(const CRequest& req)
{
	trade::CReqParam param;
	trade::CSubmitParam& params = param.m_param.emplace<trade::CSubmitParam>();
	params.m_order.m_requestId = SourceId(req);
	std::string strSide = req.GetExtraData("side");
	std::int64_t nScale = 0;
	if (("buy" != strSide && "sell" != strSide) || "limit" != req.GetExtraData("order_type") || !ReadId(req.GetExtraData("client_order_id"), params.m_order.m_clientId) || !utility::to_number(req.GetExtraData("price"), params.m_order.m_nPrice) || !utility::to_number(req.GetExtraData("quantity"), params.m_order.m_nQuantity) || !utility::to_number(req.GetExtraData("price_scale"), nScale) || (0 > nScale) || (6 < nScale))
	{
		net::SendError(req.GetConnectionId(), req, 1202, "invalid limit order parameters");
		return false;
	}
	params.m_order.m_side = "buy" == strSide ? trade::Side::Buy : trade::Side::Sell;
	params.m_order.m_nPriceScale = static_cast<int>(nScale);
	params.m_order.m_strSecurity = req.GetExtraData("security");
	utility::to_number(req.GetExtraData("strategy_id"), params.m_order.m_strategyId);
	utility::to_number(req.GetExtraData("signal_id"), params.m_order.m_signalId);
	return SendRequest(req, std::move(param));
}

bool CTradeService::HandleOrderCancel(const CRequest& req)
{
	trade::CReqParam param;
	trade::CCancelParam& params = param.m_param.emplace<trade::CCancelParam>();
	if (!ReadId(req.GetExtraData("order_id"), params.m_orderId))
	{
		net::SendError(req.GetConnectionId(), req, 1202, "order_id required");
		return false;
	}
	return SendRequest(req, std::move(param));
}

bool CTradeService::HandlePositionQuery(const CRequest& req)
{
	trade::CReqParam param;
	param.m_param = trade::CPositionsQueryParam{};
	return SendRequest(req, std::move(param));
}

bool CTradeService::HandleOrderQuery(const CRequest& req)
{
	trade::CReqParam param;
	param.m_param = trade::COrdersQueryParam{};
	return SendRequest(req, std::move(param));
}

bool CTradeService::HandleDealQuery(const CRequest& req)
{
	trade::CReqParam param;
	param.m_param = trade::CDealsQueryParam{};
	return SendRequest(req, std::move(param));
}

std::shared_ptr<trade::ITradeSession> CTradeService::FindCounter(const CRequest& req, _TyAccount& account) const
{
	if (!ReadAccountIdentity(req, account))
	{
		net::SendError(req.GetConnectionId(), req, 1202, "broker_id/account required");
		return nullptr;
	}
	std::shared_ptr<trade::ITradeSession> counter;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		const auto mIter = m_accounts.find(account);
		if (m_accounts.end() != mIter && TradeAccountState::Connected == mIter->second.m_info.m_state)
		{
			counter = mIter->second.m_session;
		}
	}
	if (nullptr == counter)
	{
		net::SendError(req.GetConnectionId(), req, 2, "Account is not logged in");
	}
	return counter;
}

bool CTradeService::SendRequest(const CRequest& req, trade::CReqParam&& param)
{
	param.m_requestId = SourceId(req);
	std::shared_ptr<trade::ITradeSession> counter = FindCounter(req, param.m_accountIndex);
	if (nullptr == counter)
	{
		return false;
	}
	if (trade::CSubmitParam* submit = std::get_if<trade::CSubmitParam>(&param.m_param); nullptr != submit)
	{
		submit->m_order.m_accountIndex = param.m_accountIndex;
	}
	net::_TyConnectionId connection = req.GetConnectionId();
	std::uint64_t requestId = req.GetId();
	_TyRequest type = req.GetType();
	_TyAccount account = param.m_accountIndex;
	trade::CResult queued = counter->SendRequest(std::move(param), [this, counter, account, connection, requestId, type](const trade::CCounterReply& reply)
	{
		if (_TyRequest::TRADE_ACCOUNT_DISCONNECT == type && reply.m_result.Ok())
		{
			{
				std::lock_guard<std::mutex> lock(m_mtx_accounts);
				const auto mIter = m_accounts.find(account);
				if (m_accounts.end() != mIter && counter == mIter->second.m_session)
				{
					mIter->second.m_info.m_state = TradeAccountState::Disconnected;
				}
			}
			counter->Close();
		}
		CRequest response;
		response.SetId(requestId);
		response.SetType(type);
		trade::protocol::WriteReply(reply, response);
		net::SendRequest(connection, response);
	});
	if (!queued.Ok())
	{
		net::SendError(connection, req, static_cast<int>(queued.m_error), queued.m_strMessage);
	}
	return queued.Ok();
}

void CTradeService::OnEvent(const trade::CCountEvent& event)
{
	_TyRequestNotify handler;
	CTradeAccountInfo info;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		const auto account = m_accounts.find(event.m_accountIndex);
		if (m_accounts.end() == account)
		{
			return;
		}
		if (trade::EventTy::Session == event.m_kind)
		{
			account->second.m_info.m_state = 0 != event.m_funds.m_nState ? static_cast<TradeAccountState>(event.m_funds.m_nState) : (event.m_funds.m_bConnected ? TradeAccountState::Connected : TradeAccountState::Disconnected);
		}
		info = account->second.m_info;
		handler = m_handler_push;
	}
	if (nullptr != handler)
	{
		handler(trade::protocol::Push(event, info));
	}
}

void CTradeService::SetPushHandler(_TyRequestNotify&& handler)
{
	std::lock_guard<std::mutex> lock(m_mtx_accounts);
	m_handler_push = std::move(handler);
}

trade::CCounterFactory& CTradeService::CounterFactory()
{
	return m_counterFactory;
}
