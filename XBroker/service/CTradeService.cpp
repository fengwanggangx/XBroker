#include "CTradeService.h"
#include "../counter/TradeProtocol.h"
#include "../common/utility.h"
#include "../common/container.h"
#include "../ini/CINIHandler.h"
#include "../counter/CSimulatedDispatcher.h"
#include "../counter/zhongtai/CZhongTaiDispatcher.h"
#include "../database/CDBEngine.h"
#include "../database/IDataBase.h"
#include "../request/request.pb.h"
#include <rapidjson/document.h>
#include <fstream>
#include <iterator>
#include <charconv>
#include <limits>

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
	m_pendingThread = std::jthread(std::bind_front(&CTradeService::PendingLoop, this));
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
	trade::CResult result = InitializeCounters();
	if (!result.Ok())
	{
		m_strLastError = result.m_strMessage;
		return false;
	}
	return LoadBrokers();
}

void CTradeService::Stop()
{
	{
		std::lock_guard<std::mutex> lock(m_mtx_pending);
		m_bPendingStopping = true;
	}
	m_pendingThread.request_stop();
	m_condition_pending.notify_all();
	if (m_pendingThread.joinable())
	{
		m_pendingThread.join();
	}
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
	// Stop also covers partial initialization before registration with the factory.
	if (nullptr != m_dispatcher_zhongtai)
	{
		m_dispatcher_zhongtai->Stop();
	}
	if (nullptr != m_dispatcher_simulated)
	{
		m_dispatcher_simulated->Stop();
	}
	FailPendingRequests();
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		m_accounts.clear();
		m_logging.clear();
		m_retired.clear();
	}
	m_dispatcher_zhongtai.reset();
	m_dispatcher_simulated.reset();
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

bool CTradeService::OnRequest(const CRequest& req)
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
		SendError(req.GetConnectionId(), req, 3, "Unsupported trade request");
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
	return SendResponse(req.GetConnectionId(), response);
}

bool CTradeService::HandleAccountLogin(const CRequest& req)
{
	_TyAccount account;
	if (!ReadAccountIdentity(req, account) || req.GetExtraData("password").empty())
	{
		SendError(req.GetConnectionId(), req, 1202, "broker_id/site/account/password required");
		return false;
	}
	const auto broker = m_brokers.find(account.first);
	if (m_brokers.end() == broker || !container::with(broker->second.m_sites, req.GetExtraData("site")))
	{
		SendError(req.GetConnectionId(), req, 3, "Unknown broker or site");
		return false;
	}
	if ("simulated" == broker->second.m_strAdapter)
	{
		db::_TyDBPtr pDB = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr == pDB)
		{
			SendError(req.GetConnectionId(), req, 2, "模拟账户数据库不可用");
			return false;
		}
		std::string strSql = "SELECT account FROM table_account WHERE broker_id=" + std::to_string(account.first) +
			" AND account=" + utility::Utf8Literal(account.second) +
			" AND password_hash=UNHEX(SHA2(CONCAT(password_salt,UNHEX('" + utility::ToHex(req.GetExtraData("password")) + "')),256)) AND status=1 LIMIT 1";
		const db::CTable& table = pDB->ExecQuery(strSql);
		if (table.m_columns.empty())
		{
			SendError(req.GetConnectionId(), req, 2, "模拟账户数据库不可用");
			return false;
		}
		if (table.m_rows.empty())
		{
			SendError(req.GetConnectionId(), req, 1002, "模拟交易账户或密码错误");
			return false;
		}
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
		SendError(req.GetConnectionId(), req, 6, "Login in progress or service stopping");
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
		return SendResponse(req.GetConnectionId(), response);
	}
	std::shared_ptr<trade::ITradeSession> counter = m_counterFactory.Create({ account, broker->second.m_strAdapter });
	if (nullptr == counter)
	{
		{
			std::lock_guard<std::mutex> lock(m_mtx_accounts);
			m_logging.erase(account);
		}
		SendError(req.GetConnectionId(), req, 3, "Adapter unavailable");
		return false;
	}
	counter->SetReplyHandler(m_counter_reply);
	if (nullptr != m_counter_event)
	{
		counter->SetEventHandler(m_counter_event);
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
		SendError(req.GetConnectionId(), req, 2, "Service stopping");
		return false;
	}
	trade::CReqParam login;
	login.m_accountIndex = account;
	login.m_requestId = SourceId(req);
	login.m_param = trade::CLoginParam{ req.GetExtraData("password"), req.GetExtraData("site") };
	return SendCounterRequest(req, counter, std::move(login));
}

void CTradeService::FinishLogin(const CPendingCounterRequest& pending, const trade::CCounterReply& reply)
{
	const _TyAccount& account = pending.m_account;
	const std::string& brokerName = pending.m_brokerName;
	net::_TyConnectionId connection = pending.m_connection;
	std::uint64_t requestId = pending.m_requestId;
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
	info.m_strSite = pending.m_site;
	info.m_strName = info.m_strAccount;
	info.m_state = TradeAccountState::Connected;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		const auto mIter = m_logging.find(account);
		std::shared_ptr<trade::ITradeSession> counter = m_logging.end() == mIter ? nullptr : mIter->second;
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
	SendResponse(connection, response);
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
	return SendResponse(req.GetConnectionId(), response);
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
		SendError(req.GetConnectionId(), req, 1202, "invalid limit order parameters");
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
		SendError(req.GetConnectionId(), req, 1202, "order_id required");
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
		SendError(req.GetConnectionId(), req, 1202, "broker_id/account required");
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
		SendError(req.GetConnectionId(), req, 2, "Account is not logged in");
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
	return SendCounterRequest(req, counter, std::move(param));
}

void CTradeService::OnCounterEvent(const trade::CCountEvent& ev)
{
	_TyRequestNotify handler;
	CTradeAccountInfo info;
	{
		std::lock_guard<std::mutex> lock(m_mtx_accounts);
		const auto account = m_accounts.find(ev.m_accountIndex);
		if (m_accounts.end() == account || 0 == ev.m_sessionId || nullptr == account->second.m_session || account->second.m_session->GetId() != ev.m_sessionId)
		{
			return;
		}
		if (trade::EventTy::Session == ev.m_kind)
		{
			account->second.m_info.m_state = 0 != ev.m_funds.m_nState ? static_cast<TradeAccountState>(ev.m_funds.m_nState) : (ev.m_funds.m_bConnected ? TradeAccountState::Connected : TradeAccountState::Disconnected);
		}
		info = account->second.m_info;
		handler = m_handler_push;
	}
	if (nullptr != handler)
	{
		handler(trade::protocol::Push(ev, info));
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

trade::CResult CTradeService::InitializeCounters()
{
	trade::CCounterFactory& creator = CounterFactory();
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
	m_dispatcher_simulated->SetDepthSubscriptionHandler([this](const std::string& security, bool subscribe)
	{
		return nullptr != m_handler_depth && m_handler_depth(security, subscribe);
	});
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

bool CTradeService::OnHQMarketReply(const CRequest& req)
{
	return nullptr != m_dispatcher_simulated && m_dispatcher_simulated->OnMarketResponse(req);
}

void CTradeService::OnHQMarketState(SessionState state, const std::string& message)
{
	if (nullptr != m_dispatcher_simulated)
	{
		m_dispatcher_simulated->OnSessionState(state, message);
	}
}

void CTradeService::SetReplyHandler(_TyReplyHandler&& handler)
{
	m_handler_reply = std::move(handler);
}

void CTradeService::SetDepthHandler(_TyDepthHandler&& handler)
{
	m_handler_depth = std::move(handler);
}

bool CTradeService::SendResponse(net::_TyConnectionId id, const CRequest& req) const
{
	return nullptr != m_handler_reply && m_handler_reply(id, req);
}

void CTradeService::SendError(net::_TyConnectionId id, const CRequest& req, int code, const std::string& message) const
{
	CRequest response = Response(req);
	response.SetErrorInfo(code, message);
	SendResponse(id, response);
}

bool CTradeService::SendCounterRequest(const CRequest& req, const std::shared_ptr<trade::ITradeSession>& counter, trade::CReqParam&& param)
{
	CPendingCounterRequest pending;
	pending.m_connection = req.GetConnectionId();
	pending.m_requestId = req.GetId();
	pending.m_sourceId = SourceId(req);
	pending.m_type = req.GetType();
	pending.m_operation = param.GetOperation();
	pending.m_account = param.m_accountIndex;
	pending.m_session = counter;
	pending.m_deadline = std::chrono::steady_clock::now() + (_TyRequest::TRADE_ACCOUNT_LOGIN == pending.m_type ? std::chrono::seconds(120) : std::chrono::seconds(30));
	if (_TyRequest::TRADE_ACCOUNT_LOGIN == pending.m_type)
	{
		pending.m_brokerName = m_brokers.at(pending.m_account.first).m_strName;
		pending.m_site = req.GetExtraData("site");
	}
	trade::CResult result;
	{
		std::lock_guard<std::mutex> lock(m_mtx_pending);
		if (m_bPendingStopping || nullptr == m_counter_reply || 4096 <= m_pending.size() || (std::numeric_limits<std::uint64_t>::max)() == m_nextCorrelationId)
		{
			result = { trade::Error::Unavailable, "Counter requests unavailable or pending limit reached" };
		}
		else
		{
			pending.m_correlationId = ++m_nextCorrelationId;
			param.m_correlationId = pending.m_correlationId;
			m_pending.emplace(pending.m_correlationId, pending);
		}
	}
	if (result.Ok())
	{
		// Store before sending: an adapter is allowed to respond synchronously.
		try
		{
			result = counter->SendRequest(std::move(param));
		}
		catch (...)
		{
			result = { trade::Error::UnknownResult, "Counter send failed; reconcile before retry" };
		}
	}
	if (!result.Ok())
	{
		trade::CCounterReply reply;
		reply.m_accountIndex = pending.m_account;
		reply.m_requestId = pending.m_sourceId;
		reply.m_correlationId = pending.m_correlationId;
		reply.m_operation = pending.m_operation;
		reply.m_result = result;
		if (0 != pending.m_correlationId)
		{
			OnCounterReply(reply);
		}
		else
		{
			FinishPending(pending, reply);
		}
	}
	return result.Ok();
}

void CTradeService::OnCounterReply(const trade::CCounterReply& reply)
{
	CPendingCounterRequest pending;
	{
		std::lock_guard<std::mutex> lock(m_mtx_pending);
		const auto mIter = m_pending.find(reply.m_correlationId);
		if (m_pending.end() == mIter || mIter->second.m_account != reply.m_accountIndex || mIter->second.m_operation != reply.m_operation)
		{
			return;
		}
		pending = std::move(mIter->second);
		m_pending.erase(mIter);
	}
	FinishPending(pending, reply);
}

void CTradeService::FinishPending(const CPendingCounterRequest& pending, const trade::CCounterReply& reply)
{
	if (_TyRequest::TRADE_ACCOUNT_LOGIN == pending.m_type)
	{
		FinishLogin(pending, reply);
		return;
	}
	if (_TyRequest::TRADE_ACCOUNT_DISCONNECT == pending.m_type && reply.m_result.Ok())
	{
		{
			std::lock_guard<std::mutex> lock(m_mtx_accounts);
			const auto mIter = m_accounts.find(pending.m_account);
			if (m_accounts.end() != mIter && pending.m_session == mIter->second.m_session)
			{
				mIter->second.m_info.m_state = TradeAccountState::Disconnected;
			}
		}
		pending.m_session->Close();
	}
	CRequest response;
	response.SetId(pending.m_requestId);
	response.SetType(pending.m_type);
	trade::protocol::WriteReply(reply, response);
	SendResponse(pending.m_connection, response);
}

void CTradeService::PendingLoop(std::stop_token token)
{
	while (!token.stop_requested())
	{
		{
			std::unique_lock<std::mutex> lock(m_mtx_pending);
			m_condition_pending.wait_for(lock, token, std::chrono::milliseconds(250), []()
			{
				return false;
			});
		}
		if (!token.stop_requested())
		{
			ExpirePendingRequests();
		}
	}
}

void CTradeService::ExpirePendingRequests()
{
	std::vector<CPendingCounterRequest> expired;
	{
		std::lock_guard<std::mutex> lock(m_mtx_pending);
		std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
		for (auto mIter = m_pending.begin(); m_pending.end() != mIter;)
		{
			if (now >= mIter->second.m_deadline)
			{
				expired.emplace_back(std::move(mIter->second));
				mIter = m_pending.erase(mIter);
			}
			else
			{
				++mIter;
			}
		}
	}
	for (const auto& pending : expired)
	{
		trade::CCounterReply reply;
		reply.m_accountIndex = pending.m_account;
		reply.m_requestId = pending.m_sourceId;
		reply.m_correlationId = pending.m_correlationId;
		reply.m_operation = pending.m_operation;
		reply.m_result = { trade::Operation::Submit == pending.m_operation || trade::Operation::Cancel == pending.m_operation ? trade::Error::UnknownResult : trade::Error::Timeout, "Counter request timed out; reconcile before retry" };
		FinishPending(pending, reply);
	}
}

void CTradeService::FailPendingRequests()
{
	std::unordered_map<std::uint64_t, CPendingCounterRequest> pendingRequests;
	{
		std::lock_guard<std::mutex> lock(m_mtx_pending);
		pendingRequests.swap(m_pending);
	}
	for (const auto& item : pendingRequests)
	{
		const CPendingCounterRequest& pending = item.second;
		trade::CCounterReply reply;
		reply.m_accountIndex = pending.m_account;
		reply.m_requestId = pending.m_sourceId;
		reply.m_operation = pending.m_operation;
		reply.m_result = { trade::Error::Unavailable, "Counter service stopped; reconcile before retry" };
		FinishPending(pending, reply);
	}
}

void CTradeService::SetCounterReplyHandler(_TyCounterReplyHandler&& handler)
{
	m_counter_reply = std::move(handler);
}

void CTradeService::SetCounterEventHandler(_TyCounterEventHandler&& handler)
{
	m_counter_event = std::move(handler);
}
