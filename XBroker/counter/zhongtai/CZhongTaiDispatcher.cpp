#include "CZhongTaiDispatcher.h"
#include "ZhongTaiConvert.h"
#include "../TradeProtocol.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include "../../common/container.h"
using namespace trade;
using namespace trade::zhongtai;

namespace
{
	constexpr Operation Queries[]{ Operation::Orders, Operation::Trades, Operation::Positions, Operation::Funds };
	bool Terminal(Status status)
	{
		return Status::Filled == status || Status::Cancelled == status || Status::Expired == status || Status::Rejected == status;
	}

	void Invoke(ITradeSession::_TyReplyHandler& handler, const CCounterReply& reply)
	{
		if (nullptr != handler)
		{
			try
			{
				handler(reply);
			}
			catch (...)
			{
			}
		}
	}
} // namespace

CZhongTaiDispatcher::CZhongTaiDispatcher(CConfig config, std::unique_ptr<IZhongTaiApi> api, std::unique_ptr<IZhongTaiStore> store) : m_config(std::move(config)), m_api(std::move(api)), m_store(std::move(store))
{
}

CZhongTaiDispatcher::~CZhongTaiDispatcher()
{
	Stop();
}

void CZhongTaiDispatcher::Start()
{
	if (nullptr == m_api || nullptr == m_store || m_worker.joinable())
	{
		throw std::runtime_error("invalid XTP gateway initialization");
	}
	m_api->SetHandler(std::bind_front(&CZhongTaiDispatcher::Receive, this));
	m_worker = std::thread(std::bind_front(&CZhongTaiDispatcher::Run, this));
	m_worker_id = m_worker.get_id();
}

void CZhongTaiDispatcher::Stop()
{
	{
		std::lock_guard<std::mutex> lock(m_queue_mutex);
		m_stopping = true;
	}
	m_condition.notify_all();
	if (m_worker_id == std::this_thread::get_id())
	{
		return;
	}
	std::lock_guard<std::mutex> stopLock(m_stop_mutex);
	if (m_worker.joinable())
	{
		m_worker.join();
	}
	if (nullptr != m_api)
	{
		m_api->Stop();
	}
	std::vector<std::shared_ptr<CZhongTaiSession>> sessions;
	{
		std::lock_guard<std::mutex> lock(m_counters_mutex);
		for (const auto& item : m_counters)
		{
			std::shared_ptr<CZhongTaiSession> session = item.second.lock();
			if (nullptr != session)
			{
				sessions.emplace_back(std::move(session));
			}
		}
		m_counters.clear();
		m_instances.clear();
	}
	for (const auto& session : sessions)
	{
		session->Close();
	}
}

std::shared_ptr<ITradeSession> CZhongTaiDispatcher::Create(const CCounterConfig& config)
{
	std::lock_guard<std::mutex> stopLock(m_stop_mutex);
	{
		std::lock_guard<std::mutex> lock(m_queue_mutex);
		if (m_stopping)
		{
			return nullptr;
		}
	}
	std::unique_lock<std::mutex> lock(m_counters_mutex);
	std::shared_ptr<CZhongTaiSession> counter = std::make_shared<CZhongTaiSession>(this, config.m_account_index, ++m_next_instance);
	std::shared_ptr<CZhongTaiSession> retired;
	const auto previous = m_counters.find(config.m_account_index);
	if (m_counters.end() != previous)
	{
		retired = previous->second.lock();
	}
	m_counters.insert_or_assign(config.m_account_index, counter);
	m_instances.insert_or_assign(config.m_account_index, m_next_instance);
	lock.unlock();
	if (nullptr != retired)
	{
		retired->Close();
	}
	return counter;
}

bool CZhongTaiDispatcher::IsCurrent(const _TyAccount& account, std::uint64_t instance)
{
	std::lock_guard<std::mutex> lock(m_counters_mutex);
	const auto mIter = m_instances.find(account);
	return m_instances.end() != mIter && instance == mIter->second;
}

CResult CZhongTaiDispatcher::Send(CReqParam request, ITradeSession::_TyReplyHandler handler, std::uint64_t instance)
{
	if (!IsCurrent(request.m_account_index, instance))
	{
		return { Error::Unavailable, "retired account session" };
	}
	std::lock_guard<std::mutex> lock(m_queue_mutex);
	if (m_stopping || m_config.m_queue_limit <= m_queue.size())
	{
		return { Error::Unavailable, "XTP gateway stopped or request queue full" };
	}
	if (nullptr != handler)
	{
		std::shared_ptr<std::atomic_bool> completed = std::make_shared<std::atomic_bool>(false);
		handler = [callback = std::move(handler), completed](const CCounterReply& reply)
		{
			if (!completed->exchange(true))
			{
				callback(reply);
			}
		};
	}
	m_queue.emplace_back(CWork{ std::move(request), instance, std::move(handler) });
	m_condition.notify_one();
	return {};
}

void CZhongTaiDispatcher::Receive(CApiEvent event)
{
	std::lock_guard<std::mutex> lock(m_queue_mutex);
	if (m_stopping)
	{
		return;
	}
	if (m_config.m_queue_limit <= m_queue.size())
	{
		m_overflow = true;
	}
	else
	{
		m_queue.emplace_back(std::move(event));
	}
	m_condition.notify_one();
}

void CZhongTaiDispatcher::Snapshot(CSession& session)
{
	session.m_snapshot.m_orders.clear();
	session.m_snapshot.m_trades.clear();
	for (const auto& item : session.m_orders)
	{
		if (session.m_journal.trading_day() == item.second.m_day)
		{
			session.m_snapshot.m_orders.emplace_back(item.second);
		}
	}
	for (const auto& item : session.m_trades)
	{
		if (session.m_journal.trading_day() == item.second.m_day)
		{
			session.m_snapshot.m_trades.emplace_back(item.second);
		}
	}
	session.m_snapshot.m_ready = session.m_ready;
	session.m_snapshot.m_version = session.m_journal.sequence();
	session.m_snapshot.m_time = Now();
	session.m_snapshot.m_account.m_account_index = session.m_account;
	std::shared_ptr<CZhongTaiSession> counter = session.m_counter.lock();
	if (nullptr != counter)
	{
		counter->SetSnapshot(session.m_snapshot);
	}
}

bool CZhongTaiDispatcher::Persist(CSession& session)
{
	session.m_journal.clear_orders();
	session.m_journal.clear_trades();
	for (const auto& item : session.m_orders)
	{
		protocol::WriteOrder(item.second, *session.m_journal.add_orders());
	}
	for (const auto& item : session.m_trades)
	{
		protocol::WriteTrade(item.second, *session.m_journal.add_trades());
	}
	CResult result = m_store->Save(session.m_account, session.m_journal);
	if (!result.Ok())
	{
		session.m_disabled = true;
		session.m_ready = false;
		session.m_snapshot.m_account.m_funds_valid = false;
		session.m_snapshot.m_account.m_connected = false;
		session.m_snapshot.m_account.m_state = 2;
		Snapshot(session);
		CCountEvent invalid;
		invalid.m_account_index = session.m_account;
		invalid.m_kind = EventTy::Session;
		invalid.m_funds = session.m_snapshot.m_account;
		invalid.m_sequence = 0;
		std::shared_ptr<CZhongTaiSession> counter = session.m_counter.lock();
		if (nullptr != counter && session.m_published)
		{
			counter->Publish(invalid);
		}
		return false;
	}
	return true;
}

void CZhongTaiDispatcher::Publish(CSession& session, CCountEvent event)
{
	event.m_account_index = session.m_account;
	event.m_funds = session.m_snapshot.m_account;
	event.m_account_valid = true;
	event.m_sequence = session.m_journal.sequence() + 1;
	session.m_journal.set_sequence(event.m_sequence);
	protocol::WriteEvent(event, *session.m_journal.add_events());
	if (!Persist(session))
	{
		throw std::runtime_error("XTP event persistence failed");
	}
	Snapshot(session);
	if (!session.m_published)
	{
		session.m_initial_events.emplace_back(std::move(event));
		return;
	}
	std::shared_ptr<CZhongTaiSession> counter = session.m_counter.lock();
	if (nullptr != counter)
	{
		counter->Publish(event);
	}
}

void CZhongTaiDispatcher::Reply(CSession& session, CWork& work, const CResult& result, std::uint64_t orderId)
{
	CCounterReply reply;
	reply.m_account_index = session.m_account;
	reply.m_request_id = work.m_request.m_request_id;
	reply.m_correlation_id = work.m_request.m_correlation_id;
	reply.m_operation = work.m_request.GetOperation();
	reply.m_result = result;
	reply.m_version = session.m_journal.sequence();
	reply.m_time = Now();
	switch (reply.m_operation)
	{
	case Operation::Funds:
		reply.m_data = session.m_snapshot.m_account;
		break;
	case Operation::Positions:
		reply.m_data = session.m_snapshot.m_positions;
		break;
	case Operation::Orders:
		reply.m_data = session.m_snapshot.m_orders;
		break;
	case Operation::Trades:
		reply.m_data = session.m_snapshot.m_trades;
		break;
	default:
		reply.m_data = COperationReceipt{ orderId };
		break;
	}
	Invoke(work.m_handler, reply);
}

void CZhongTaiDispatcher::Fail(CSession& session, const CResult& result, bool disable)
{
	if (session.m_login.has_value())
	{
		disable = true;
	}
	session.m_ready = false;
	session.m_disabled = session.m_disabled || disable;
	if (disable)
	{
		session.m_password.clear();
	}
	session.m_snapshot.m_account.m_connected = false;
	session.m_snapshot.m_account.m_funds_valid = false;
	session.m_snapshot.m_account.m_state = session.m_disabled ? 2 : 5;
	if (m_query.has_value() && m_query->m_account == session.m_account)
	{
		if (m_query->m_work.has_value())
		{
			Reply(session, *m_query->m_work, result);
		}
		m_query.reset();
	}
	if (0 != session.m_session)
	{
		m_api->Logout(session.m_session);
		session.m_session = 0;
	}
	for (auto& work : session.m_queries)
	{
		Reply(session, work, result);
	}
	session.m_queries.clear();
	if (session.m_login.has_value())
	{
		Reply(session, *session.m_login, result);
		session.m_login.reset();
	}
	session.m_buffer.clear();
	session.m_initial_events.clear();
	int delays[]{ 1, 2, 4, 8, 16, 30 };
	session.m_retry_at = Now() + 1000 * delays[std::min(session.m_attempt++, 5)];
	Snapshot(session);
	if (session.m_published)
	{
		CCountEvent event;
		event.m_kind = EventTy::Session;
		try
		{
			Publish(session, event);
		}
		catch (...)
		{
			event.m_funds = session.m_snapshot.m_account;
			event.m_account_index = session.m_account;
			event.m_sequence = 0;
			std::shared_ptr<CZhongTaiSession> counter = session.m_counter.lock();
			if (nullptr != counter)
			{
				counter->Publish(event);
			}
		}
	}
	if (session.m_disabled)
	{
		m_store->Release(session.m_account);
	}
}

void CZhongTaiDispatcher::Login(CSession& session)
{
	const auto vIter = std::find_if(m_config.m_sites.begin(), m_config.m_sites.end(), [&session](const CSite& site)
	{
		return session.m_account.first == site.m_broker_id && session.m_site == site.m_name;
	});
	if (m_config.m_sites.end() == vIter)
	{
		Fail(session, { Error::Rejected, "unknown ZhongTai server-side site" }, true);
		return;
	}
	CResult loaded = m_store->Load(session.m_account, session.m_journal);
	if (!loaded.Ok())
	{
		Fail(session, loaded, true);
		return;
	}
	session.m_orders.clear();
	session.m_trades.clear();
	for (const auto& row : session.m_journal.orders())
	{
		COrder order = ReadOrder(row);
		session.m_orders.emplace(order.m_order_id, std::move(order));
	}
	for (const auto& row : session.m_journal.trades())
	{
		CTrade trade = ReadTrade(row);
		session.m_trades.emplace(TradeKey(trade), std::move(trade));
	}
	session.m_lease_at = Now() + 5000;
	CLoginReply login = m_api->Login(*vIter, session.m_account.second, session.m_password);
	if (!login.m_result.Ok() || 0 == login.m_session)
	{
		Fail(session, login.m_result.Ok() ? CResult{ Error::Unavailable, "zero XTP session" } : login.m_result, login.m_auth_failure);
		return;
	}
	if (!m_used_sessions.emplace(login.m_session).second)
	{
		m_api->Logout(login.m_session);
		Fail(session, { Error::Unavailable, "SDK reused session ID; restart required" }, true);
		return;
	}
	session.m_session = login.m_session;
	++session.m_generation;
	session.m_journal.set_trading_day(login.m_day);
	session.m_ready = false;
	session.m_sync_step = 0;
	session.m_refresh = false;
	session.m_buffer.clear();
	session.m_snapshot.m_account.m_state = 4;
	session.m_snapshot.m_account.m_connected = false;
	session.m_snapshot.m_account.m_funds_valid = false;
	Snapshot(session);
	if (session.m_published)
	{
		CCountEvent event;
		event.m_kind = EventTy::Session;
		Publish(session, std::move(event));
	}
}

void CZhongTaiDispatcher::Handle(CWork work)
{
	const _TyAccount& account = work.m_request.m_account_index;
	const auto mIter = m_sessions.find(account);
	Operation operation = work.m_request.GetOperation();
	if (!IsCurrent(account, work.m_instance))
	{
		CCounterReply reply;
		reply.m_account_index = account;
		reply.m_request_id = work.m_request.m_request_id;
		reply.m_correlation_id = work.m_request.m_correlation_id;
		reply.m_operation = operation;
		reply.m_result = { Error::Unavailable, "retired account session" };
		Invoke(work.m_handler, reply);
		return;
	}
	if (Operation::Login == operation)
	{
		if (m_sessions.end() != mIter && (mIter->second.m_login.has_value() || 0 != mIter->second.m_session))
		{
			Reply(mIter->second, work, { Error::Conflict, "account already logged in or connecting" });
			return;
		}
		CSession& session = m_sessions[account];
		session.m_account = account;
		session.m_instance = work.m_instance;
		{
			std::lock_guard<std::mutex> lock(m_counters_mutex);
			const auto counter = m_counters.find(account);
			if (m_counters.end() != counter)
			{
				session.m_counter = counter->second;
			}
		}
		const CLoginParam& parameters = std::get<CLoginParam>(work.m_request.m_param);
		session.m_published = false;
		session.m_initial_events.clear();
		session.m_site = parameters.m_site;
		session.m_password = parameters.m_password;
		session.m_disabled = false;
		session.m_login = std::move(work);
		CResult loaded = m_store->Load(account, session.m_journal);
		if (!loaded.Ok())
		{
			Fail(session, loaded, true);
			return;
		}
		session.m_orders.clear();
		session.m_trades.clear();
		for (const auto& row : session.m_journal.orders())
		{
			COrder order = ReadOrder(row);
			session.m_orders.emplace(order.m_order_id, std::move(order));
		}
		for (const auto& row : session.m_journal.trades())
		{
			CTrade trade = ReadTrade(row);
			session.m_trades.emplace(TradeKey(trade), std::move(trade));
		}
		Login(session);
		return;
	}
	if (m_sessions.end() == mIter)
	{
		CCounterReply reply;
		reply.m_account_index = account;
		reply.m_request_id = work.m_request.m_request_id;
		reply.m_correlation_id = work.m_request.m_correlation_id;
		reply.m_operation = operation;
		reply.m_result = { Error::NotFound, "XTP account not logged in" };
		Invoke(work.m_handler, reply);
		return;
	}
	CSession& session = mIter->second;
	if (session.m_instance != work.m_instance)
	{
		Reply(session, work, { Error::Unavailable, "retired account session" });
		return;
	}
	if (Operation::Disconnect == operation)
	{
		Fail(session, { Error::Unavailable, "account disconnected" }, true);
		session.m_password.clear();
		session.m_snapshot.m_account.m_state = 0;
		session.m_disabled = true;
		Reply(session, work, {});
		m_store->Release(account);
		return;
	}
	if (!session.m_ready)
	{
		Reply(session, work, { Error::Unavailable, "XTP account is not synchronized" });
		return;
	}
	if (Operation::Submit == operation)
	{

		COrder order = std::get<CSubmitParam>(work.m_request.m_param).m_order;
		order.m_account_index = account;
		order.m_request_id = work.m_request.m_request_id;
		order.m_day = session.m_journal.trading_day();
		if (!IsStock(order.m_security) || 0 == order.m_client_id || 0 >= order.m_price || 0 >= order.m_quantity || 0 > order.m_price_scale || 4 < order.m_price_scale || (Side::Buy != order.m_side && Side::Sell != order.m_side))
		{
			Reply(session, work, { Error::Rejected, "only valid Shanghai/Shenzhen cash stock limit orders supported" });
			return;
		}
		while (4 > order.m_price_scale)
		{
			if (std::numeric_limits<std::int64_t>::max() / 10 < order.m_price)
			{
				Reply(session, work, { Error::Rejected, "price overflow" });
				return;
			}
			order.m_price *= 10;
			++order.m_price_scale;
		}
		for (const auto& item : session.m_orders)
		{
			if (order.m_day == item.second.m_day && order.m_client_id == item.second.m_client_id)
			{
				CResult result = SameOrder(order, item.second) ? CResult{} : CResult{ Error::Conflict, "client order ID reused" };
				if (result.Ok() && (Status::Unknown == item.second.m_status || Status::Pending == item.second.m_status))
				{
					result = { Error::UnknownResult, "previous order outcome requires reconciliation" };
				}
				if (result.Ok() && Status::Rejected == item.second.m_status)
				{
					result = { Error::Rejected, item.second.m_error_message };
				}
				Reply(session, work, result, item.first);
				return;
			}
		}
		if (!session.m_snapshot.m_account.m_funds_valid)
		{
			Reply(session, work, { Error::Unavailable, "funds snapshot invalid" });
			return;
		}
		order.m_order_id = session.m_journal.next_order() + 1;
		session.m_journal.set_next_order(order.m_order_id);
		if (std::numeric_limits<std::uint32_t>::max() < order.m_order_id)
		{
			Reply(session, work, { Error::Unavailable, "XTP client reference exhausted" });
			return;
		}
		order.m_sdk_client_id = static_cast<std::uint32_t>(order.m_order_id);
		order.m_status = Status::Unknown;
		order.m_fee_known = false;
		order.m_frozen_known = false;
		session.m_orders.emplace(order.m_order_id, order);
		if (!Persist(session))
		{
			Reply(session, work, { Error::Unavailable, "cannot persist send intent" });
			Fail(session, { Error::Unavailable, "cannot persist send intent" }, true);
			return;
		}
		std::uint64_t brokerId = 0;
		CResult result;
		try
		{
			result = m_api->Submit(session.m_session, order, brokerId);
		}
		catch (...)
		{
			Reply(session, work, { Error::UnknownResult, "XTP submit outcome unknown" }, order.m_order_id);
			Fail(session, { Error::UnknownResult, "reconcile uncertain submit" });
			return;
		}
		COrder& stored = session.m_orders.at(order.m_order_id);
		session.m_snapshot.m_account.m_funds_valid = false;
		session.m_dirty_financials = true;
		session.m_refresh_at = 0;
		if (!result.Ok() || 0 == brokerId)
		{
			if (result.Ok())
			{
				result = { Error::Rejected, "XTP did not return an order ID" };
			}
			stored.m_status = Status::Rejected;
			stored.m_error_message = result.m_message;
		}
		else
		{
			stored.m_broker_order_id = std::to_string(brokerId);
			stored.m_status = Status::Pending;
		}
		CCountEvent event;
		event.m_kind = EventTy::Order;
		event.m_order = stored;
		event.m_request_id = order.m_request_id;
		try
		{
			Publish(session, std::move(event));
		}
		catch (...)
		{
			Reply(session, work, { Error::UnknownResult, "order sent but journal update failed" }, order.m_order_id);
			Fail(session, { Error::Unavailable, "storage failure" }, true);
			return;
		}
		Reply(session, work, result, order.m_order_id);
		return;
	}
	if (Operation::Cancel == operation)
	{
		std::uint64_t id = std::get<CCancelParam>(work.m_request.m_param).m_order_id;
		const auto order = session.m_orders.find(id);
		if (session.m_orders.end() == order || session.m_journal.trading_day() != order->second.m_day)
		{
			Reply(session, work, { Error::NotFound, "order not found for current trading day" });
			return;
		}
		if (Terminal(order->second.m_status))
		{
			Reply(session, work, {}, id);
			return;
		}
		if (order->second.m_broker_order_id.empty())
		{
			Reply(session, work, { Error::UnknownResult, "broker order ID not confirmed" }, id);
			return;
		}
		if (order->second.m_cancel_pending)
		{
			Reply(session, work, {}, id);
			return;
		}
		order->second.m_cancel_pending = true;
		if (!Persist(session))
		{
			Reply(session, work, { Error::Unavailable, "cannot persist cancel intent" }, id);
			Fail(session, { Error::Unavailable, "cannot persist cancel intent" }, true);
			return;
		}
		CResult result;
		try
		{
			result = m_api->Cancel(session.m_session, std::stoull(order->second.m_broker_order_id));
		}
		catch (...)
		{
			Reply(session, work, { Error::UnknownResult, "XTP cancel outcome unknown" }, id);
			Fail(session, { Error::UnknownResult, "reconcile uncertain cancel" });
			return;
		}
		if (!result.Ok())
		{
			order->second.m_cancel_pending = false;
		}
		CCountEvent event;
		event.m_kind = EventTy::Order;
		event.m_order = order->second;
		event.m_request_id = work.m_request.m_request_id;
		try
		{
			Publish(session, std::move(event));
		}
		catch (...)
		{
			Reply(session, work, { Error::UnknownResult, "cancel sent but journal update failed" }, id);
			Fail(session, { Error::Unavailable, "storage failure" }, true);
			return;
		}
		Reply(session, work, result, id);
		return;
	}
	if (Operation::Connect == operation)
	{
		Reply(session, work, {});
		return;
	}
	if (session.m_queries.size() >= m_config.m_queue_limit)
	{
		Reply(session, work, { Error::Unavailable, "query queue full" });
		return;
	}
	session.m_queries.emplace_back(std::move(work));
}

void CZhongTaiDispatcher::Apply(CSession& session, const CApiEvent& event)
{
	if (!event.m_has_row)
	{
		return;
	}

	if (ApiEventType::Order == event.m_type)
	{
		COrder order = event.m_order;
		if (0 == order.m_day)
		{
			order.m_day = session.m_journal.trading_day();
		}
		order.m_account_index = session.m_account;
		const auto mIter = std::find_if(session.m_orders.begin(), session.m_orders.end(), [&](const auto& item)
		{
			return order.m_day == item.second.m_day && (order.m_broker_order_id == item.second.m_broker_order_id || (0 != order.m_sdk_client_id && order.m_sdk_client_id == item.second.m_sdk_client_id));
		});
		if (session.m_orders.end() != mIter)
		{
			COrder& previous = mIter->second;
			if (previous.m_broker_time > order.m_broker_time)
			{
				return;
			}
			if (Terminal(previous.m_status) && !Terminal(order.m_status))
			{
				return;
			}
			order.m_order_id = previous.m_order_id;
			order.m_client_id = previous.m_client_id;
			order.m_request_id = previous.m_request_id;
			order.m_strategy_id = previous.m_strategy_id;
			order.m_signal_id = previous.m_signal_id;
			order.m_filled = std::max(order.m_filled, previous.m_filled);
			order.m_cancel_pending = !Terminal(order.m_status) && previous.m_cancel_pending;
			if (!Terminal(order.m_status) && 0 < order.m_filled)
			{
				order.m_status = order.m_filled >= order.m_quantity ? Status::Filled : Status::PartiallyFilled;
			}
			request::TradeOrderRecord oldWire, newWire;
			protocol::WriteOrder(previous, oldWire);
			protocol::WriteOrder(order, newWire);
			if (oldWire.SerializeAsString() == newWire.SerializeAsString())
			{
				return;
			}
		}
		else
		{
			order.m_order_id = session.m_journal.next_order() + 1;
			session.m_journal.set_next_order(order.m_order_id);
		}
		if (0 == event.m_request_id && (ApiEventType::Order == event.m_type || ApiEventType::Trade == event.m_type))
		{
			session.m_snapshot.m_account.m_funds_valid = false;
			session.m_dirty_financials = true;
		}

		session.m_orders.insert_or_assign(order.m_order_id, order);
		CCountEvent output;
		output.m_kind = EventTy::Order;
		output.m_order = order;
		output.m_request_id = order.m_request_id;
		Publish(session, std::move(output));
		session.m_refresh_at = 0;
	}
	else if (ApiEventType::Trade == event.m_type)
	{
		CTrade trade = event.m_trade;
		trade.m_account_index = session.m_account;
		std::string key = TradeKey(trade);
		if (container::with(session.m_trades, key))
		{
			return;
		}
		const auto mIter = std::find_if(session.m_orders.begin(), session.m_orders.end(), [&](const auto& item)
		{
			return trade.m_day == item.second.m_day && trade.m_broker_order_id == item.second.m_broker_order_id;
		});
		if (session.m_orders.end() == mIter)
		{
			if (session.m_buffer.size() >= m_config.m_queue_limit)
			{
				throw std::runtime_error("unmatched trade buffer overflow");
			}
			session.m_buffer.emplace_back(event);
			session.m_refresh_at = 0;
			return;
		}
		if (0 == event.m_request_id && (ApiEventType::Order == event.m_type || ApiEventType::Trade == event.m_type))
		{
			session.m_snapshot.m_account.m_funds_valid = false;
			session.m_dirty_financials = true;
		}

		trade.m_order_id = mIter->first;
		trade.m_trade_id = session.m_journal.next_trade() + 1;
		session.m_journal.set_next_trade(trade.m_trade_id);
		session.m_trades.emplace(key, trade);
		CCountEvent output;
		output.m_kind = EventTy::Deal;
		output.m_trade = trade;
		Publish(session, std::move(output));
		COrder& order = mIter->second;
		std::int64_t filled = 0;
		for (const auto& item : session.m_trades)
		{
			if (item.second.m_order_id == order.m_order_id)
			{
				filled += item.second.m_quantity;
			}
		}
		order.m_filled = std::max(order.m_filled, filled);
		if (!Terminal(order.m_status))
		{
			order.m_status = order.m_filled >= order.m_quantity ? Status::Filled : Status::PartiallyFilled;
		}
		if (Terminal(order.m_status))
		{
			order.m_cancel_pending = false;
		}
		CCountEvent change;
		change.m_kind = EventTy::Order;
		change.m_order = order;
		Publish(session, std::move(change));
		session.m_refresh_at = 0;
	}
	else if (ApiEventType::Funds == event.m_type)
	{
		session.m_dirty_financials = false;
		session.m_snapshot.m_account = event.m_funds;
		session.m_snapshot.m_account.m_account_index = session.m_account;
		session.m_snapshot.m_account.m_connected = session.m_ready;
		session.m_snapshot.m_account.m_state = session.m_ready ? 1 : 4;
		CCountEvent output;
		output.m_kind = EventTy::Funds;
		Publish(session, std::move(output));
	}
}

void CZhongTaiDispatcher::Query(CSession& session, Operation operation, bool sync, std::optional<CWork> work)
{
	if (std::numeric_limits<int>::max() == m_next_query)
	{
		if (work.has_value())
		{
			Reply(session, *work, { Error::Unavailable, "query IDs exhausted" });
		}
		Fail(session, { Error::Unavailable, "XTP query IDs exhausted; restart required" }, true);
		return;
	}
	CQuery query;
	query.m_account = session.m_account;
	query.m_session = session.m_session;
	query.m_generation = session.m_generation;
	query.m_operation = operation;
	query.m_id = m_next_query++;
	query.m_sync = sync;
	query.m_deadline = Now() + m_config.m_query_timeout_ms;
	query.m_work = std::move(work);
	m_query = std::move(query);
	CResult result = m_api->Query(session.m_session, operation, m_query->m_id, 0);
	m_next_query_at = Now() + m_config.m_query_interval_ms;
	if (!result.Ok())
	{
		Fail(session, result);
	}
}

void CZhongTaiDispatcher::FinishQuery()
{
	CQuery query = std::move(*m_query);
	m_query.reset();
	CSession& session = m_sessions.at(query.m_account);
	try
	{
		if (Operation::Funds == query.m_operation && query.m_data.empty())
		{
			CResult result{ Error::Unavailable, "empty XTP asset response" };
			if (query.m_work.has_value())
			{
				Reply(session, *query.m_work, result);
			}
			Fail(session, result);
			return;
		}
		if (Operation::Positions == query.m_operation)
		{
			session.m_snapshot.m_positions.clear();
			for (const auto& event : query.m_data)
			{
				CPosition position = event.m_position;
				position.m_account_index = session.m_account;
				session.m_snapshot.m_positions.emplace_back(std::move(position));
			}
			for (const auto& position : session.m_snapshot.m_positions)
			{
				CCountEvent event;
				event.m_kind = EventTy::Position;
				event.m_position = position;
				Publish(session, std::move(event));
			}
		}
		else
		{
			for (const auto& event : query.m_data)
			{
				Apply(session, event);
			}
		}

		if (Operation::Orders == query.m_operation)
		{
			std::set<std::string> observed;
			for (const auto& row : query.m_data)
			{
				observed.emplace(row.m_order.m_broker_order_id);
			}
			for (auto& item : session.m_orders)
			{
				COrder& order = item.second;
				if (session.m_journal.trading_day() == order.m_day && !Terminal(order.m_status) && Status::Unknown != order.m_status && !order.m_broker_order_id.empty() && observed.end() == observed.find(order.m_broker_order_id))
				{
					order.m_status = Status::Unknown;
					order.m_error_message = "order absent from completed broker query; outcome uncertain";
					CCountEvent event;
					event.m_kind = EventTy::Order;
					event.m_order = order;
					Publish(session, std::move(event));
				}
			}
		}
		// Apply buffered live updates after each query, preserving cumulative values and deduplicating trades.
		std::vector<CApiEvent> buffer = std::move(session.m_buffer);
		session.m_buffer.clear();
		for (const auto& event : buffer)
		{
			Apply(session, event);
		}
		if (query.m_sync)
		{
			++session.m_sync_step;
			if (4 == session.m_sync_step)
			{
				if (session.m_dirty_financials)
				{
					session.m_sync_step = 2;
					return;
				}

				if (!session.m_buffer.empty())
				{
					Fail(session, { Error::Unavailable, "unmatched trade requires order reconciliation" });
					return;
				}
				session.m_ready = true;
				session.m_attempt = 0;
				session.m_refresh = false;
				session.m_refresh_at = Now() + m_config.m_refresh_ms;
				session.m_snapshot.m_account.m_connected = true;
				session.m_snapshot.m_account.m_state = 1;
				for (auto& item : session.m_orders)
				{
					if (session.m_journal.trading_day() == item.second.m_day && item.second.m_broker_order_id.empty())
					{
						item.second.m_status = Status::Unknown;
					}
				}
				if (!Persist(session))
				{
					Fail(session, { Error::Unavailable, "journal unavailable" }, true);
					return;
				}
				Snapshot(session);
				if (session.m_login.has_value())
				{
					Reply(session, *session.m_login, {});
					session.m_login.reset();
				}
				session.m_published = true;
				std::shared_ptr<CZhongTaiSession> counter = session.m_counter.lock();
				if (nullptr != counter)
				{
					for (const auto& event : session.m_initial_events)
					{
						counter->Publish(event);
					}
				}
				session.m_initial_events.clear();
				CCountEvent event;
				event.m_kind = EventTy::Session;
				Publish(session, std::move(event));
			}
		}
		Snapshot(session);
		if (query.m_work.has_value())
		{
			Reply(session, *query.m_work, {});
		}
	}
	catch (const std::exception& error)
	{
		CResult result{ Error::Unavailable, error.what() };
		if (query.m_work.has_value())
		{
			Reply(session, *query.m_work, result);
		}
		Fail(session, result, true);
	}
}

void CZhongTaiDispatcher::Handle(CApiEvent event)
{
	if (ApiEventType::Error == event.m_type && 0 == event.m_session)
	{
		if (event.m_result.Ok())
		{
			return;
		}
		for (auto& item : m_sessions)
		{
			Fail(item.second, event.m_result);
		}
		return;
	}
	const auto mIter = std::find_if(m_sessions.begin(), m_sessions.end(), [&](const auto& item)
	{
		return 0 != event.m_session && event.m_session == item.second.m_session;
	});
	if (m_sessions.end() == mIter)
	{
		return;
	}
	CSession& session = mIter->second;
	if (ApiEventType::Disconnected == event.m_type)
	{
		Fail(session, event.m_result);
		return;
	}
	if (ApiEventType::CancelError == event.m_type || ApiEventType::UnknownOrder == event.m_type)
	{
		for (auto& item : session.m_orders)
		{
			if (std::to_string(event.m_cancel_order) == item.second.m_broker_order_id && session.m_journal.trading_day() == item.second.m_day)
			{
				item.second.m_cancel_pending = false;
				if (ApiEventType::UnknownOrder == event.m_type)
				{
					item.second.m_status = Status::Unknown;
				}
				item.second.m_error_message = event.m_result.m_message;
				CCountEvent output;
				output.m_kind = EventTy::Order;
				output.m_order = item.second;
				try
				{
					Publish(session, std::move(output));
				}
				catch (...)
				{
					Fail(session, { Error::Unavailable, "cancel callback journal update failed" }, true);
					return;
				}
			}
		}
		return;
	}
	if (0 != event.m_request_id)
	{
		if (!m_query.has_value() || event.m_request_id != m_query->m_id || session.m_generation != m_query->m_generation || session.m_account != m_query->m_account)
		{
			return;
		}
		if (!event.m_result.Ok())
		{
			Fail(session, event.m_result);
			return;
		}
		if (event.m_has_row)
		{
			m_query->m_data.emplace_back(event);
			++m_query->m_rows;
		}
		if (event.m_last)
		{
			if ((Operation::Orders == m_query->m_operation || Operation::Trades == m_query->m_operation) && 1000 == m_query->m_rows)
			{
				if (event.m_reference <= m_query->m_reference)
				{
					Fail(session, { Error::Unavailable, "non-advancing XTP page reference" });
					return;
				}
				m_query->m_reference = event.m_reference;
				m_query->m_rows = 0;
				m_query->m_page_pending = true;
				m_next_query_at = Now() + m_config.m_query_interval_ms;
				// Deadline covers the entire paginated query, not each page.
				return;
			}
			FinishQuery();
		}
		return;
	}
	if (!event.m_result.Ok())
	{
		Fail(session, event.m_result);
		return;
	}
	if (!session.m_ready || (m_query.has_value() && m_query->m_account == session.m_account))
	{
		if (session.m_buffer.size() >= m_config.m_queue_limit)
		{
			Fail(session, { Error::Unavailable, "XTP synchronization buffer overflow" });
			return;
		}
		session.m_buffer.emplace_back(std::move(event));
		return;
	}
	try
	{
		Apply(session, event);
		Snapshot(session);
	}
	catch (const std::exception& error)
	{
		Fail(session, { Error::Unavailable, error.what() }, true);
	}
}

void CZhongTaiDispatcher::Tick()
{
	std::int64_t now = Now();
	if (m_overflow.exchange(false))
	{
		for (auto& item : m_sessions)
		{
			Fail(item.second, { Error::Unavailable, "XTP callback queue overflow; resynchronization required" });
		}
	}
	if (m_query.has_value() && now >= m_query->m_deadline)
	{
		Fail(m_sessions.at(m_query->m_account), { Error::Timeout, "XTP query timed out" });
	}
	if (m_query.has_value() && m_query->m_page_pending && now >= m_next_query_at)
	{
		CSession& session = m_sessions.at(m_query->m_account);
		if (std::numeric_limits<int>::max() == m_next_query)
		{
			Fail(session, { Error::Unavailable, "query ID exhausted" }, true);
		}
		else
		{
			m_query->m_id = m_next_query++;
			m_query->m_page_pending = false;
			CResult result = m_api->Query(session.m_session, m_query->m_operation, m_query->m_id, m_query->m_reference);
			if (!result.Ok())
			{
				Fail(session, result);
			}
		}
	}
	for (auto& item : m_sessions)
	{
		CSession& session = item.second;
		if (session.m_disabled)
		{
			continue;
		}
		if (now >= session.m_lease_at)
		{
			CResult renewed = m_store->Save(session.m_account, session.m_journal);
			session.m_lease_at = now + 5000;
			if (!renewed.Ok())
			{
				Fail(session, renewed, true);
				continue;
			}
		}
		if (0 == session.m_session)
		{
			if (!session.m_password.empty() && now >= session.m_retry_at)
			{
				Login(session);
			}
			continue;
		}
		if (session.m_ready && now >= session.m_refresh_at && !session.m_refresh)
		{
			session.m_refresh = true;
			session.m_sync_step = 0;
			session.m_snapshot.m_account.m_funds_valid = false;
			Snapshot(session);
		}
	}
	if (m_query.has_value() || now < m_next_query_at || m_sessions.empty())
	{
		return;
	}
	auto mIter = m_sessions.upper_bound(m_last_scheduled);
	for (std::size_t count = 0; count < m_sessions.size(); ++count)
	{
		if (m_sessions.end() == mIter)
		{
			mIter = m_sessions.begin();
		}
		CSession& session = mIter->second;
		if (!session.m_disabled && 0 != session.m_session)
		{
			if (!session.m_ready || session.m_refresh)
			{
				m_last_scheduled = mIter->first;
				Query(session, Queries[session.m_sync_step], true);
				return;
			}
			if (!session.m_queries.empty())
			{
				CWork work = std::move(session.m_queries.front());
				session.m_queries.pop_front();
				Operation operation = work.m_request.GetOperation();
				m_last_scheduled = mIter->first;
				Query(session, operation, false, std::move(work));
				return;
			}
		}
		++mIter;
	}
}

void CZhongTaiDispatcher::Run()
{
	for (;;)
	{
		std::optional<std::variant<CWork, CApiEvent>> work;
		bool closing = false;
		{
			std::unique_lock<std::mutex> lock(m_queue_mutex);
			m_condition.wait_for(lock, std::chrono::milliseconds(20), [this]()
			{
				return m_stopping || !m_queue.empty() || m_overflow.load();
			});
			closing = m_stopping;
			if (!m_queue.empty())
			{
				work = std::move(m_queue.front());
				m_queue.pop_front();
			}
			else if (m_stopping)
			{
				break;
			}
		}
		ITradeSession::_TyReplyHandler fallback;
		CCounterReply failure;
		if (work.has_value())
		{
			if (CWork* incoming = std::get_if<CWork>(&*work); nullptr != incoming)
			{
				fallback = incoming->m_handler;
				failure.m_account_index = incoming->m_request.m_account_index;
				failure.m_request_id = incoming->m_request.m_request_id;
				failure.m_correlation_id = incoming->m_request.m_correlation_id;
				failure.m_operation = incoming->m_request.GetOperation();
			}
		}
		if (closing && work.has_value())
		{
			if (CWork* queued = std::get_if<CWork>(&*work); nullptr != queued && Operation::Disconnect != queued->m_request.GetOperation())
			{
				failure.m_result = { Error::Unavailable, "gateway stopping; queued request not sent" };
				Invoke(fallback, failure);
				continue;
			}
		}
		try
		{
			if (work.has_value())
			{
				std::visit([this](auto&& item)
				{
					Handle(std::move(item));
				}, std::move(*work));
			}
			if (!closing)
			{
				Tick();
			}
		}
		catch (const std::exception& error)
		{
			failure.m_result = { Operation::Submit == failure.m_operation || Operation::Cancel == failure.m_operation ? Error::UnknownResult : Error::Unavailable, error.what() };
			Invoke(fallback, failure);

			for (auto& item : m_sessions)
			{
				Fail(item.second, { Error::Unavailable, error.what() }, true);
			}
		}
	}
	for (auto& item : m_sessions)
	{
		Fail(item.second, { Error::Unavailable, "gateway stopped" }, true);
		item.second.m_password.clear();
		m_store->Release(item.first);
	}
}
