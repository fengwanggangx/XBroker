#include "../zhongtai/CZhongTaiDispatcher.h"
#include "../zhongtai/ZhongTaiConvert.h"
#include <future>
#include <iostream>
#include <stdexcept>
#include <atomic>
using namespace trade;
using namespace trade::zhongtai;
namespace
{
	void Check(bool condition, const char* message)
	{
		if (!condition)
		{
			throw std::runtime_error(message);
		}
	}
	struct CMemory
	{
		std::map<_TyAccount, request::ZhongTaiJournal> m_data;
		std::atomic_bool m_fail{ false };
	};
	class CStore final : public IZhongTaiStore
	{
	public:
		explicit CStore(std::shared_ptr<CMemory> memory) : m_memory(std::move(memory))
		{
		}
		CResult Load(const _TyAccount& account, request::ZhongTaiJournal& journal) override
		{
			if (m_memory->m_fail)
			{
				return { Error::Unavailable, "store failure" };
			}
			journal = m_memory->m_data[account];
			return {};
		}
		CResult Save(const _TyAccount& account, request::ZhongTaiJournal& journal) override
		{
			if (m_memory->m_fail)
			{
				return { Error::Unavailable, "store failure" };
			}
			journal.set_revision(journal.revision() + 1);
			m_memory->m_data[account] = journal;
			return {};
		}
		void Release(const _TyAccount&) override
		{
		}

	private:
		std::shared_ptr<CMemory> m_memory;
	};
	class CApi final : public IZhongTaiApi
	{
	public:
		void Stop() override
		{
			std::lock_guard<std::mutex> lock(m_mtx_state);
			m_handler = {};
		}
		void SetHandler(_TyHandler&& handler) override
		{
			std::lock_guard<std::mutex> lock(m_mtx_state);
			m_handler = std::move(handler);
		}
		CLoginReply Login(const CSite& site, const std::string& account, const std::string& password) override
		{
			std::lock_guard<std::mutex> lock(m_mtx_state);
			++m_logins;
			if ("bad" == password)
			{
				return { { Error::Rejected, "bad password" }, 0, 0, true };
			}
			std::uint64_t session = ++m_session;
			m_accounts[session] = account;
			m_hosts.push_back(site.m_host);
			return { {}, session, 20261030, false };
		}
		void Logout(std::uint64_t) override
		{
		}
		CResult Query(std::uint64_t session, Operation operation, int requestId, std::int64_t reference) override
		{
			if (m_queryFailure.exchange(false))
			{
				return { Error::Unavailable, "query rejected" };
			}
			if (m_hang.exchange(false))
			{
				return {};
			}
			CApiEvent event;
			event.m_session = session;
			event.m_requestId = requestId;
			event.m_last = true;
			if (Operation::Funds == operation)
			{
				event.m_type = ApiEventType::Funds;
				event.m_hasRow = true;
				event.m_funds.m_nBalance = 10000000;
				event.m_funds.m_nAvailable = 9000000;
				event.m_funds.m_nFrozen = 1000000;
				event.m_funds.m_bExplicitAvailable = true;
			}
			if (Operation::Positions == operation)
			{
				event.m_type = ApiEventType::Position;
			}
			if (Operation::Orders == operation)
			{
				event.m_type = ApiEventType::Order;
				std::vector<COrder> orders;
				{
					std::lock_guard<std::mutex> lock(m_mtx_state);
					orders = m_orders[m_accounts.at(session)];
				}
				if (m_pages.load() && !orders.empty())
				{
					++m_pageCalls;
					if (0 == reference)
					{
						for (int index = 0; index < 1000; ++index)
						{
							CApiEvent row = event;
							row.m_last = false;
							row.m_order = orders.front();
							row.m_hasRow = true;
							Emit(row);
						}
						event.m_reference = 1000;
					}
					Emit(event);
					return {};
				}
				for (const auto& order : orders)
				{
					CApiEvent row = event;
					row.m_last = false;
					row.m_order = order;
					row.m_hasRow = true;
					Emit(row);
				}
			}
			if (Operation::Trades == operation)
			{
				event.m_type = ApiEventType::Trade;
			}
			Emit(event);
			return {};
		}
		CResult Submit(std::uint64_t session, const COrder& order, std::uint64_t& brokerId) override
		{
			if (m_throwSubmit.exchange(false))
			{
				throw std::runtime_error("injected unknown send");
			}
			COrder row = order;
			{
				std::lock_guard<std::mutex> lock(m_mtx_state);
				brokerId = ++m_order;
				row.m_strBrokerOrderId = std::to_string(brokerId);
				row.m_status = m_rejectNext.exchange(false) ? Status::Rejected : Status::Accepted;
				row.m_nBrokerTime = 20261030100000000LL;
				m_orders[m_accounts.at(session)].push_back(row);
				++m_submits;
			}
			CApiEvent event;
			event.m_session = session;
			event.m_type = ApiEventType::Order;
			event.m_order = row;
			event.m_hasRow = true;
			Emit(event);
			return {};
		}
		CResult Cancel(std::uint64_t session, std::uint64_t id) override
		{
			CApiEvent event;
			event.m_session = session;
			if (m_cancelFailure.exchange(false))
			{
				event.m_type = ApiEventType::CancelError;
				event.m_cancelOrder = id;
				event.m_result = { Error::Rejected, "cancel rejected" };
				Emit(event);
				return {};
			}
			event.m_type = ApiEventType::Order;
			{
				std::lock_guard<std::mutex> lock(m_mtx_state);
				for (auto& order : m_orders[m_accounts.at(session)])
				{
					if (std::to_string(id) == order.m_strBrokerOrderId)
					{
						order.m_status = Status::Cancelled;
						order.m_nBrokerTime++;
						event.m_order = order;
						event.m_hasRow = true;
					}
				}
			}
			Emit(event);
			return {};
		}

		void Emit(CApiEvent event)
		{
			_TyHandler handler;
			{
				std::lock_guard<std::mutex> lock(m_mtx_state);
				handler = m_handler;
			}
			if (nullptr != handler)
			{
				handler(std::move(event));
			}
		}
		std::atomic_int m_submits{ 0 }, m_logins{ 0 };
		std::atomic_bool m_queryFailure{ false }, m_hang{ false }, m_pages{ false }, m_throwSubmit{ false }, m_cancelFailure{ false }, m_rejectNext{ false };
		std::atomic_int m_pageCalls{ 0 };
		std::mutex m_mtx_state;
		std::vector<std::string> m_hosts;
		std::map<std::string, std::vector<COrder>> m_orders;

	private:
		_TyHandler m_handler;
		std::uint64_t m_session{ 0 }, m_order{ 1000 };
		std::map<std::uint64_t, std::string> m_accounts;
	};
	CCounterReply Send(const std::shared_ptr<ITradeSession>& counter, CReqParam request)
	{
		std::shared_ptr<std::promise<CCounterReply>> promise = std::make_shared<std::promise<CCounterReply>>();
		std::future<CCounterReply> future = promise->get_future();
		Check(counter->SendRequest(std::move(request), [promise](const CCounterReply& reply)
		{
			promise->set_value(reply);
		}).Ok(),
			"request enqueued");
		Check(std::future_status::ready == future.wait_for(std::chrono::seconds(4)), "request callback completed");
		return future.get();
	}

	CReqParam Request(const _TyAccount& account, Operation operation)
	{
		CReqParam request;
		request.m_accountIndex = account;
		request.m_requestId = 1;
		if (Operation::Login == operation)
		{
			request.m_param = CLoginParam{ "secret", 1 == account.first ? "primary" : "secondary" };
		}
		else if (Operation::Disconnect == operation)
		{
			request.m_param = CDisconnectParam{};
		}
		else if (Operation::Submit == operation)
		{
			COrder order;
			order.m_clientId = 1;
			order.m_strSecurity = "600000.SSE";
			order.m_nPrice = 1000;
			order.m_nQuantity = 100;
			request.m_param = CSubmitParam{ order };
		}
		else if (Operation::Cancel == operation)
		{
			request.m_param = CCancelParam{};
		}
		else if (Operation::Orders == operation)
		{
			request.m_param = COrdersQueryParam{};
		}
		else if (Operation::Positions == operation)
		{
			request.m_param = CPositionsQueryParam{};
		}
		else if (Operation::Trades == operation)
		{
			request.m_param = CDealsQueryParam{};
		}
		else
		{
			request.m_param = CFundsQueryParam{};
		}
		return request;
	}

	void Run()
	{
		Check(IsStock("600000.SSE") && IsStock("000001.SZSE") && !IsStock("510300.SSE"), "ordinary stocks only");
		Check(1793325600000LL == BrokerTime(20261030100000000LL), "Shanghai epoch conversion");
		CConfig config;
		config.m_queryIntervalMs = 0;
		// Allow the 1000-row pagination fixture to run under debug iterators.
		config.m_queryTimeoutMs = 1000;
		config.m_refreshMs = 60000;
		config.m_sites = { { 1, "primary", "host-a", "", 1 }, { 2, "secondary", "host-b", "", 2 } };
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		std::unique_ptr<CApi> api = std::make_unique<CApi>();
		CApi* fake = api.get();
		std::unique_ptr<CZhongTaiDispatcher> gateway = std::make_unique<CZhongTaiDispatcher>(config, std::move(api), std::make_unique<CStore>(memory));
		gateway->Start();
		Check(gateway->IsInitialized(), "started dispatcher is initialized");
		Check(Error::Conflict == gateway->Initialize().m_error, "initialization preserves a running injected dispatcher");
		_TyAccount a{ 1, "000123" }, b{ 2, "000123" };
		std::shared_ptr<ITradeSession> first = gateway->Create({ a, "zhongtai_xtp" }), second = gateway->Create({ b, "zhongtai_xtp" });
		Check(Send(first, Request(a, Operation::Login)).m_result.Ok(), "first login and synchronization");
		Check(Send(second, Request(b, Operation::Login)).m_result.Ok(), "second login different site");
		{
			std::lock_guard<std::mutex> lock(fake->m_mtx_state);
			Check(2 == fake->m_hosts.size() && "host-a" == fake->m_hosts[0] && "host-b" == fake->m_hosts[1], "independent login endpoints");
		}
		Check(!Send(first, Request(a, Operation::Login)).m_result.Ok(), "duplicate login rejected");
		CCounterReply funds = Send(first, Request(a, Operation::Funds));
		Check(funds.m_result.Ok() && 9000000 == std::get<CFunds>(funds.m_data).m_nAvailable, "explicit broker available cash");
		CCounterReply accepted = Send(first, Request(a, Operation::Submit));
		Check(accepted.m_result.Ok(), "submit");
		std::uint64_t id = std::get<COperationReceipt>(accepted.m_data).m_orderId;
		Send(first, Request(a, Operation::Orders));
		Check(Send(first, Request(a, Operation::Submit)).m_result.Ok() && 1 == fake->m_submits.load(), "duplicate submit does not send twice");
		fake->m_pages = true;
		Check(Send(first, Request(a, Operation::Orders)).m_result.Ok(), "paginated query completes");
		fake->m_pages = false;
		Check(2 <= fake->m_pageCalls.load() && 0 == fake->m_pageCalls.load() % 2, "full page follows continuation cursor");
		CReqParam conflict = Request(a, Operation::Submit);
		std::get<CSubmitParam>(conflict.m_param).m_order.m_nQuantity = 200;
		Check(Error::Conflict == Send(first, conflict).m_result.m_error, "conflicting duplicate rejected");
		CApiEvent trade;
		trade.m_type = ApiEventType::Trade;
		trade.m_session = 1;
		trade.m_hasRow = true;
		trade.m_trade.m_strBrokerOrderId = "1001";
		trade.m_trade.m_strBrokerTradeId = "same-exec";
		trade.m_trade.m_nDay = 20261030;
		trade.m_trade.m_market = 1;
		trade.m_trade.m_reportIndex = 1;
		trade.m_trade.m_nQuantity = 20;
		trade.m_trade.m_nPrice = 100000;
		trade.m_trade.m_nPriceScale = 4;
		trade.m_trade.m_strSecurity = "600000.SSE";
		fake->Emit(trade);
		fake->Emit(trade);
		trade.m_trade.m_reportIndex = 2;
		fake->Emit(trade);
		Send(first, Request(a, Operation::Orders));
		CAccountData snapshot = first->Snapshot();
		Check(2 == snapshot.m_trades.size() && 40 == snapshot.m_orders.front().m_nFilled, "report index dedup and partial fills");
		Check(second->Snapshot().m_orders.empty(), "account isolation");
		CReqParam cancel = Request(a, Operation::Cancel);
		std::get<CCancelParam>(cancel.m_param).m_orderId = id;
		fake->m_cancelFailure = true;
		Check(Send(first, cancel).m_result.Ok(), "cancel send receipt");
		Send(first, Request(a, Operation::Orders));
		Check(!first->Snapshot().m_orders.front().m_bCancelPending && Status::PartiallyFilled == first->Snapshot().m_orders.front().m_status, "cancel rejection keeps active order");
		Check(Send(first, cancel).m_result.Ok(), "cancel queued");
		Send(first, Request(a, Operation::Orders));
		Check(Status::Cancelled == first->Snapshot().m_orders.front().m_status, "terminal cancellation");
		CApiEvent stale;
		stale.m_type = ApiEventType::Order;
		stale.m_session = 1;
		stale.m_hasRow = true;
		stale.m_order = first->Snapshot().m_orders.front();
		stale.m_order.m_status = Status::Accepted;
		stale.m_order.m_nBrokerTime = 1;
		fake->Emit(stale);
		Send(first, Request(a, Operation::Orders));
		Check(Status::Cancelled == first->Snapshot().m_orders.front().m_status, "stale callback cannot regress terminal order");

		CApiEvent external;
		external.m_type = ApiEventType::Order;
		external.m_session = 1;
		external.m_hasRow = true;
		external.m_order = first->Snapshot().m_orders.front();
		external.m_order.m_strBrokerOrderId = "90001";
		external.m_order.m_sdkClientId = 0;
		external.m_order.m_clientId = 0;
		external.m_order.m_strategyId = 0;
		external.m_order.m_status = Status::Accepted;
		external.m_order.m_nFilled = 0;
		external.m_order.m_nBrokerTime = 20261030101000000LL;
		{
			std::lock_guard<std::mutex> lock(fake->m_mtx_state);
			fake->m_orders[a.second].push_back(external.m_order);
		}
		fake->Emit(external);
		Send(first, Request(a, Operation::Orders));
		Check(2 == first->Snapshot().m_orders.size() && 0 == first->Snapshot().m_orders.back().m_clientId, "external terminal order has no invented strategy association");
		CReqParam rejected = Request(a, Operation::Submit);
		std::get<CSubmitParam>(rejected.m_param).m_order.m_clientId = 2;
		fake->m_rejectNext = true;
		Check(Send(first, rejected).m_result.Ok(), "SDK receipt precedes rejection");
		Send(first, Request(a, Operation::Orders));
		Check(Status::Rejected == first->Snapshot().m_orders.back().m_status, "broker rejection is final");
		CReqParam filled = Request(a, Operation::Submit);
		std::get<CSubmitParam>(filled.m_param).m_order.m_clientId = 3;
		Check(Send(first, filled).m_result.Ok(), "next order after rejection");
		Send(first, Request(a, Operation::Orders));
		trade.m_trade.m_strBrokerOrderId = "1003";
		trade.m_trade.m_reportIndex = 3;
		trade.m_trade.m_nQuantity = 100;
		fake->Emit(trade);
		Send(first, Request(a, Operation::Orders));
		Check(Status::Filled == first->Snapshot().m_orders.back().m_status, "complete fill from trade callback");
		CTrade tomorrow = trade.m_trade;
		++tomorrow.m_nDay;
		Check(TradeKey(tomorrow) != TradeKey(trade.m_trade), "cross-day reports remain distinct");
		fake->m_queryFailure = true;
		Check(!Send(first, Request(a, Operation::Funds)).m_result.Ok(), "query failure is not empty success");
		std::this_thread::sleep_for(std::chrono::milliseconds(1200));
		Check(Send(first, Request(a, Operation::Funds)).m_result.Ok(), "automatic reconnect and synchronize");
		fake->Emit(stale);
		Send(first, Request(a, Operation::Orders));
		Check(Status::Cancelled == first->Snapshot().m_orders.front().m_status, "retired SDK session ignored");
		fake->m_hang = true;
		Check(Error::Timeout == Send(first, Request(a, Operation::Funds)).m_result.m_error, "query timeout completed");
		gateway->Stop();
		Check(!first->Snapshot().m_bReady, "stop invalidates snapshot");
		std::uint64_t sequence = memory->m_data[a].sequence();
		Check(0 < sequence && 4 == memory->m_data[a].orders_size(), "durable journal preserved");
		std::unique_ptr<CApi> restartApi = std::make_unique<CApi>();
		std::unique_ptr<CZhongTaiDispatcher> restarted = std::make_unique<CZhongTaiDispatcher>(config, std::move(restartApi), std::make_unique<CStore>(memory));
		restarted->Start();
		std::shared_ptr<ITradeSession> recovered = restarted->Create({ a, "zhongtai_xtp" });
		Check(Send(recovered, Request(a, Operation::Login)).m_result.Ok(), "restart recovery");
		Check(recovered->Snapshot().m_version > sequence && 3 == recovered->Snapshot().m_trades.size(), "restart preserves sequence and dedup");
		restarted->Stop();
	}

	void SessionLifecycle()
	{
		CConfig config;
		config.m_queryIntervalMs = 0;
		config.m_refreshMs = 60000;
		config.m_sites = { { 1, "primary", "host-a", "", 1 }, { 2, "secondary", "host-b", "", 2 } };
		std::unique_ptr<CZhongTaiDispatcher> dispatcher = std::make_unique<CZhongTaiDispatcher>(config, std::make_unique<CApi>(), std::make_unique<CStore>(std::make_shared<CMemory>()));
		dispatcher->Start();
		_TyAccount a{ 1, "000123" }, b{ 2, "000123" };
		std::shared_ptr<ITradeSession> first = dispatcher->Create({ a, "zhongtai_xtp" });
		std::shared_ptr<ITradeSession> second = dispatcher->Create({ b, "zhongtai_xtp" });
		Check(Send(first, Request(a, Operation::Login)).m_result.Ok(), "lifecycle first login");
		Check(Send(second, Request(b, Operation::Login)).m_result.Ok(), "lifecycle second login");
		std::atomic_int rejectedCallbacks{ 0 };
		Check(!first->SendRequest(Request(b, Operation::Funds), [&rejectedCallbacks](const CCounterReply&)
		{
			++rejectedCallbacks;
		}).Ok() && 0 == rejectedCallbacks.load(),
			"mismatch rejected without callback");
		Check(Send(first, Request(a, Operation::Disconnect)).m_result.Ok(), "business disconnect acknowledged");
		first->Close();
		Check(!first->Snapshot().m_bReady && !first->SendRequest(Request(a, Operation::Funds), {}).Ok(), "closed account unavailable");
		Check(Send(second, Request(b, Operation::Funds)).m_result.Ok(), "closing first preserves shared dispatcher");
		std::shared_ptr<ITradeSession> replacement = dispatcher->Create({ a, "zhongtai_xtp" });
		Check(Send(replacement, Request(a, Operation::Login)).m_result.Ok(), "replacement account login");
		Check(!first->SendRequest(Request(a, Operation::Submit), {}).Ok(), "retired account cannot send");
		dispatcher->Stop();
		Check(!second->SendRequest(Request(b, Operation::Funds), [&rejectedCallbacks](const CCounterReply&)
		{
			++rejectedCallbacks;
		}).Ok() && 0 == rejectedCallbacks.load(),
			"stopped dispatcher rejects without callback");
		Check(nullptr == dispatcher->Create({ a, "zhongtai_xtp" }), "stopped dispatcher cannot create sessions");
		dispatcher.reset();
		Check(!replacement->SendRequest(Request(a, Operation::Funds), {}).Ok() && !replacement->Snapshot().m_bReady, "session remains safely closed after dispatcher destruction");
	}

	void DispatcherDestruction()
	{
		std::shared_ptr<ITradeSession> session;
		_TyAccount account{ 1, "000123" };
		{
			CConfig config;
			CZhongTaiDispatcher dispatcher(config, std::make_unique<CApi>(), std::make_unique<CStore>(std::make_shared<CMemory>()));
			dispatcher.Start();
			session = dispatcher.Create({ account, "zhongtai_xtp" });
		}
		Check(!session->SendRequest(Request(account, Operation::Funds), {}).Ok() && !session->Snapshot().m_bReady, "session detached by dispatcher destructor");
		session->Close();
	}

	void Failures()
	{
		CConfig config;
		config.m_queryIntervalMs = 0;
		config.m_refreshMs = 60000;
		config.m_sites = { { 1, "primary", "host-a", "", 1 } };
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		std::unique_ptr<CApi> api = std::make_unique<CApi>();
		CApi* fake = api.get();
		std::unique_ptr<CZhongTaiDispatcher> gateway = std::make_unique<CZhongTaiDispatcher>(config, std::move(api), std::make_unique<CStore>(memory));
		gateway->Start();
		_TyAccount account{ 1, "000999" };
		std::shared_ptr<ITradeSession> counter = gateway->Create({ account, "zhongtai_xtp" });
		CReqParam bad = Request(account, Operation::Login);
		std::get<CLoginParam>(bad.m_param).m_strPassword = "bad";
		Check(!Send(counter, bad).m_result.Ok(), "authentication error rejected");
		std::this_thread::sleep_for(std::chrono::milliseconds(1100));
		Check(1 == fake->m_logins.load(), "authentication does not retry");
		Check(Send(counter, Request(account, Operation::Login)).m_result.Ok(), "manual login after failure");
		fake->m_throwSubmit = true;
		Check(Error::UnknownResult == Send(counter, Request(account, Operation::Submit)).m_result.m_error, "ambiguous send never treated as rejection");
		std::this_thread::sleep_for(std::chrono::milliseconds(1200));
		Check(Error::UnknownResult == Send(counter, Request(account, Operation::Submit)).m_result.m_error && 0 == fake->m_submits.load(), "uncertain intent not automatically resent");
		CReqParam next = Request(account, Operation::Submit);
		std::get<CSubmitParam>(next.m_param).m_order.m_clientId = 2;
		memory->m_fail = true;
		Check(!Send(counter, next).m_result.Ok() && 0 == fake->m_submits.load(), "storage failure prevents SDK send");
		gateway->Stop();
	}

	void Overflow()
	{
		CConfig config;
		config.m_queryIntervalMs = 0;
		config.m_queueLimit = 8;
		config.m_sites = { { 1, "primary", "host-a", "", 1 } };
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		std::unique_ptr<CApi> api = std::make_unique<CApi>();
		CApi* fake = api.get();
		std::unique_ptr<CZhongTaiDispatcher> gateway = std::make_unique<CZhongTaiDispatcher>(config, std::move(api), std::make_unique<CStore>(memory));
		gateway->Start();
		_TyAccount account{ 1, "001" };
		std::shared_ptr<ITradeSession> counter = gateway->Create({ account, "zhongtai_xtp" });
		Check(Send(counter, Request(account, Operation::Login)).m_result.Ok(), "overflow test login");
		Check(Send(counter, Request(account, Operation::Submit)).m_result.Ok(), "overflow test order");
		Send(counter, Request(account, Operation::Funds));
		fake->m_pages = true;
		Check(!Send(counter, Request(account, Operation::Orders)).m_result.Ok(), "callback overflow fails outstanding query");
		Check(!counter->Snapshot().m_bReady, "overflow invalidates cache");
		gateway->Stop();
	}

} // namespace
int main()
{
	try
	{
		Run();
		Failures();
		SessionLifecycle();
		DispatcherDestruction();
		Overflow();
		std::cout << "ZhongTai adapter tests passed\n";
		return 0;
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
}
