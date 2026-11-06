#include "SessionTestTools.h"
#include "CSimulatedCore.h"
#include "CSimulatedDispatcher.h"
#include <iostream>
#include <stdexcept>
#include <future>
#include <deque>
#include <atomic>
#include <limits>
using namespace trade;

static _TyAccount TestAccount(std::uint64_t number)
{
	return 0 == number ? _TyAccount{} : _TyAccount{ 1, std::to_string(number) };
}

static trade::_TyRequestParam MakeParams(trade::Operation operation)
{
	switch (operation)
	{
	case trade::Operation::Login:
		return trade::CLoginParam{};
	case trade::Operation::Connect:
		return trade::CConnectParam{};
	case trade::Operation::Disconnect:
		return trade::CDisconnectParam{};
	case trade::Operation::Submit:
		return trade::CSubmitParam{};
	case trade::Operation::Cancel:
		return trade::CCancelParam{};
	case trade::Operation::Funds:
		return trade::CFundsQueryParam{};
	case trade::Operation::Positions:
		return trade::CPositionsQueryParam{};
	case trade::Operation::Orders:
		return trade::COrdersQueryParam{};
	case trade::Operation::Trades:
		return trade::CDealsQueryParam{};
	}
	throw std::runtime_error("invalid operation");
}

namespace
{
	struct CMemory
	{
		CState m_state;
		bool m_bFail{ false }, m_bLease{ false };
	};
	class CStore final : public ICounterStore
	{
	public:
		explicit CStore(std::shared_ptr<CMemory> memory) : m_memory(std::move(memory))
		{
		}
		CResult Acquire(CState& state) override
		{
			if (m_memory->m_bLease)
			{
				return { Error::Unavailable, "lease busy" };
			}
			m_memory->m_bLease = true;
			state = m_memory->m_state;
			return {};
		}
		CResult Commit(const CState& state, std::uint64_t nExpected) override
		{
			if (m_memory->m_bFail || !m_memory->m_bLease || (nExpected != m_memory->m_state.m_sequence))
			{
				return { Error::Unavailable, "injected failure" };
			}
			ApplyChanges(m_memory->m_state, state);
			return {};
		}
		CResult Renew() override
		{
			return m_memory->m_bLease ? CResult{} : CResult{ Error::Unavailable, "lease lost" };
		}
		void Release() override
		{
			m_memory->m_bLease = false;
		}

	private:
		std::shared_ptr<CMemory> m_memory;
	};
	void Check(bool bOk, const char* pText)
	{
		if (!bOk)
		{
			throw std::runtime_error(pText);
		}
	}

	CRule Rule(int nDay)
	{
		CRule rule;
		rule.m_strVersion = "fixture-1";
		rule.m_strSecurity = "600000.SSE";
		rule.m_strBoard = "MAIN";
		rule.m_nDay = nDay;
		rule.m_nLower = 500;
		rule.m_nUpper = 2000;
		rule.m_nCommissionPpm = 100;
		rule.m_nMinimumFee = 5;
		return rule;
	}

	CReqParam Request(Operation operation, _TyAccount nAccount = TestAccount(1), std::uint64_t nClient = 1, std::int64_t nQuantity = 100, std::int64_t nPrice = 1000)
	{
		CReqParam request;
		request.m_param = MakeParams(operation);
		request.m_accountIndex = nAccount;
		request.m_requestId = nClient;
		if (Operation::Submit == operation)
		{
			std::get<trade::CSubmitParam>(request.m_param).m_order.m_clientId = nClient;
			std::get<trade::CSubmitParam>(request.m_param).m_order.m_nPrice = nPrice;
			std::get<trade::CSubmitParam>(request.m_param).m_order.m_nQuantity = nQuantity;
			std::get<trade::CSubmitParam>(request.m_param).m_order.m_strSecurity = "600000.SSE";
		}
		return request;
	}

	void Connect(CSimulatedCore& counter, const _TyAccount& nId, std::int64_t nNow)
	{
		Check(counter.Execute(Request(Operation::Connect, nId), nNow).m_result.Ok(), "connect");
	}

	class CDeferredBroker final : public ITradeSession
	{
	public:
		CResult SendRequest(CReqParam request) override
		{
			m_work.emplace_back([this, request, handler = m_handler_reply]()
			{
				CCounterReply reply;
				reply.m_requestId = request.m_requestId;
				reply.m_correlationId = request.m_correlationId;
				reply.m_accountIndex = request.m_accountIndex;
				reply.m_operation = request.GetOperation();
				switch (request.GetOperation())
				{
				case Operation::Funds:
					reply.m_data = CFunds{ TestAccount(1), 1000000, 0, true, true };
					break;
				case Operation::Positions:
					reply.m_data = std::vector<CPosition>{};
					break;
				case Operation::Orders:
					reply.m_data = std::vector<COrder>{};
					break;
				case Operation::Trades:
					reply.m_data = std::vector<CTrade>{};
					break;
				default:
					reply.m_data = COperationReceipt{};
					break;
				}
				if (Operation::Submit == request.GetOperation())
				{
					CCountEvent event;
					event.m_sessionId = GetId();
					event.m_kind = EventTy::Order;
					event.m_accountIndex = TestAccount(1);
					event.m_order = std::get<trade::CSubmitParam>(request.m_param).m_order;
					event.m_order.m_accountIndex = TestAccount(1);
					event.m_order.m_orderId = 42;
					event.m_sequence = 1;
					if (nullptr != m_handler)
					{
						m_handler(event);
					}
					reply.m_data = COperationReceipt{ 42 };
				}
				if (nullptr != handler)
				{
					handler(reply);
				}
			});
			return {};
		}
		void SetReplyHandler(_TyReplyHandler handler) override
		{
			m_handler_reply = std::move(handler);
		}
		void SetEventHandler(_TyEventHandler handler) override
		{
			m_handler = std::move(handler);
		}
		CAccountData Snapshot() const override
		{
			CAccountData snapshot;
			snapshot.m_bReady = true;
			snapshot.m_account = { TestAccount(1), 1000000, 0, true, true };
			return snapshot;
		}
		void Close() override
		{
			m_work.clear();
		}

		void Pump()
		{
			while (!m_work.empty())
			{
				std::function<void()> work = std::move(m_work.front());
				m_work.pop_front();
				work();
			}
		}

	private:
		_TyReplyHandler m_handler_reply;
		_TyEventHandler m_handler;
		std::deque<std::function<void()>> m_work;
	};
	void Contract(ITradeSession& gateway, const std::function<void()>& pump)
	{
		std::atomic_int events{ 0 };
		gateway.SetEventHandler([&events, &gateway](const CCountEvent& event)
		{
			if (EventTy::Order == event.m_kind)
			{
				Check(gateway.GetId() == event.m_sessionId, "events carry their source session id");
				++events;
			}
		});
		auto send = [&gateway, &pump](CReqParam request)
		{
			request.m_correlationId = 987654;
			std::shared_ptr<std::promise<CCounterReply>> promise = std::make_shared<std::promise<CCounterReply>>();
			std::future<CCounterReply> future = promise->get_future();
			Check(countertest::SendRequest(gateway, std::move(request), [promise](const CCounterReply& reply)
			{
				promise->set_value(reply);
			}).Ok(),
				  "request receipt");
			pump();
			Check(std::future_status::ready == future.wait_for(std::chrono::seconds(3)), "async completion");
			CCounterReply reply = future.get();
			Check(987654 == reply.m_correlationId, "fixed reply handler preserves internal correlation");
			return reply;
		};
		Check(send(Request(Operation::Connect)).m_result.Ok(), "connect contract");
		for (Operation operation : { Operation::Funds, Operation::Positions, Operation::Orders, Operation::Trades })
		{
			CCounterReply reply = send(Request(operation));
			Check(reply.m_result.Ok() && TestAccount(1) == reply.m_accountIndex && operation == reply.m_operation && !std::holds_alternative<std::monostate>(reply.m_data), "query contract");
		}
		Check(send(Request(Operation::Submit, TestAccount(1), 99)).m_result.Ok(), "submit contract");
		Check(0 < events.load(), "order event may precede reply");
		gateway.SetEventHandler({});
		gateway.Close();
	}

	void Boundaries()
	{
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		CSimulatedCore counter(std::make_unique<CStore>(memory));
		Check(counter.Start(0).Ok(), "boundary start");
		Check(counter.AddAccount(TestAccount(1), 1000000).Ok(), "boundary account");
		CCounterReply invalid = counter.Execute(Request(Operation::Funds, TestAccount(0)), 1100);
		Check(!invalid.m_result.Ok() && std::holds_alternative<std::monostate>(invalid.m_data), "zero account rejected without data");
		Check(!counter.Execute(Request(Operation::Funds, TestAccount(99)), 1100).m_result.Ok(), "unknown account rejected");
		Check(!counter.Snapshot(TestAccount(0), 1100).m_bReady && counter.Snapshot(TestAccount(0), 1100).m_orders.empty(), "zero snapshot never aggregates");
		CCounterReply empty = counter.Execute(Request(Operation::Positions, TestAccount(1)), 1100);
		Check(empty.m_result.Ok() && std::holds_alternative<std::vector<CPosition>>(empty.m_data) && std::get<std::vector<CPosition>>(empty.m_data).empty(), "typed empty positions");
		Connect(counter, TestAccount(1), 1100);
		counter.OnMarketState(true);
		Check(!counter.Execute(Request(Operation::Submit), 1100).m_result.Ok(), "missing rules");
		Check(counter.ImportRules({ Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } }).Ok(), "boundary rules");
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 1, 101), 1100).m_result.Ok(), "buy lot");
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 2, 1000000), 1100).m_result.Ok(), "insufficient cash");
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 3, 100, 2001), 1100).m_result.Ok(), "price band");
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 4, 100), 2500).m_result.Ok(), "lunch break");
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 5, (std::numeric_limits<std::int64_t>::max)()), 1100).m_result.Ok(), "overflow");
		Check(counter.Execute(Request(Operation::Submit, TestAccount(1), 6, 200), 1100).m_result.Ok(), "boundary submit");
		CDepth depth{ "600000.SSE", 1, 1100, 2, { { 990, 500 } }, { { 999, 100 }, { 1000, 100 } } };
		Check(counter.OnDepth(depth, 1100).Ok(), "multi level");
		CAccountData snapshot = counter.Snapshot(TestAccount(1), 1100);
		Check(2 == snapshot.m_trades.size() && Status::Filled == snapshot.m_orders[0].m_status && 0 == snapshot.m_account.m_nFrozen, "filled multi level");
		Check(snapshot.m_orders[0].m_nFee == 20 && snapshot.m_account.m_nBalance == 800080, "cumulative fee exact");
		Check(!counter.OnDepth(CDepth{ "600000.SSE", 2, 1000, 2, {}, { { 1000, 100 } } }, 1100).Ok(), "out of order timestamp");
		Check(!counter.OnDepth(CDepth{ "600000.SSE", 3, 0, 2, {}, { { 1000, 100 } } }, 7000).Ok(), "stale depth");
		counter.OnMarketState(false);
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 7), 1200).m_result.Ok(), "offline new order");
		Check(counter.Execute(Request(Operation::Funds), 1200).m_result.Ok(), "offline query");
		CReqParam cancel = Request(Operation::Cancel);
		std::get<trade::CCancelParam>(cancel.m_param).m_orderId = snapshot.m_orders[0].m_orderId;
		Check(counter.Execute(cancel, 1200).m_result.Ok(), "terminal cancel idempotent");
		counter.Stop();
		std::shared_ptr<CMemory> asyncMemory = std::make_shared<CMemory>();
		std::unique_ptr<CSimulatedDispatcher> dispatcher = std::make_unique<CSimulatedDispatcher>();
		CSimulatedDispatcher& gateway = *dispatcher;
		asyncMemory->m_bFail = true;
		Check(!gateway.Start(std::make_unique<CStore>(asyncMemory), []()
		{
			return 1100;
		}, {}, {}, {})
					  .Ok() &&
				  !asyncMemory->m_bLease,
			  "failed start releases dispatcher storage lease");
		asyncMemory->m_bFail = false;
		Check(gateway.Start(std::make_unique<CStore>(asyncMemory), []()
		{
			return 1100;
		}, { { TestAccount(1), 1000000, 0, false, true } }, { Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } })
				  .Ok(),
			  "gateway start");
		Check(!gateway.Start(std::make_unique<CStore>(std::make_shared<CMemory>()), []()
		{
			return 1100;
		}, {}, {}, {})
				   .Ok(),
			  "duplicate dispatcher start rejected");
		gateway.OnMarketState(true);
		std::shared_ptr<ITradeSession> session = dispatcher->Create({ TestAccount(1), "simulated" });
		Contract(*session, []()
		{
		});
		gateway.Stop();
		Check(!asyncMemory->m_bLease && gateway.Snapshot(TestAccount(1)).m_orders.empty(), "stop releases lease and clears dispatcher snapshots");
		std::shared_ptr<CMemory> restartMemory = std::make_shared<CMemory>();
		Check(gateway.Start(std::make_unique<CStore>(restartMemory), []()
		{
			return 1100;
		}, { { TestAccount(1), 2000000, 0, false, true } }, { Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } })
				  .Ok(),
			  "dispatcher restart");
		Check(2000000 == gateway.Snapshot(TestAccount(1)).m_account.m_nBalance && gateway.Snapshot(TestAccount(1)).m_orders.empty(), "restart uses fresh store");
		gateway.Stop();
		gateway.Stop();
		CDeferredBroker broker;
		Contract(broker, [&broker]()
		{
			broker.Pump();
		});
	}

	void DispatcherDestruction()
	{
		std::shared_ptr<ITradeSession> session;
		{
			CSimulatedDispatcher dispatcher;
			Check(dispatcher.Start(std::make_unique<CStore>(std::make_shared<CMemory>()), []()
			{
				return 1100;
			}, { { TestAccount(1), 1000000, 0, false, true } }, { Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } })
					  .Ok(),
				  "stack dispatcher starts");
			session = dispatcher.Create({ TestAccount(1), "simulated" });
		}
		Check(!session->Snapshot().m_bReady && !countertest::SendRequest(*session, Request(Operation::Funds), {}).Ok(), "session detached by dispatcher destructor");
		session->Close();
	}

	void SessionIsolation()
	{
		std::unique_ptr<CSimulatedDispatcher> dispatcher = std::make_unique<CSimulatedDispatcher>();
		Check(dispatcher->Start(std::make_unique<CStore>(std::make_shared<CMemory>()), []()
		{
			return 1100;
		}, { { TestAccount(1), 1000000, 0, false, true }, { TestAccount(2), 2000000, 0, false, true } }, { Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } })
				  .Ok(),
			  "session dispatcher start");
		dispatcher->OnMarketState(true);
		std::shared_ptr<ITradeSession> first = dispatcher->Create({ TestAccount(1), "simulated" });
		std::shared_ptr<ITradeSession> second = dispatcher->Create({ TestAccount(2), "simulated" });
		auto send = [](const std::shared_ptr<ITradeSession>& session, CReqParam request)
		{
			std::shared_ptr<std::promise<CCounterReply>> promise = std::make_shared<std::promise<CCounterReply>>();
			std::future<CCounterReply> future = promise->get_future();
			Check(countertest::SendRequest(*session, std::move(request), [promise](const CCounterReply& reply)
			{
				promise->set_value(reply);
			}).Ok(),
				  "session request queued");
			Check(std::future_status::ready == future.wait_for(std::chrono::seconds(3)), "session response completed");
			return future.get();
		};
		Check(send(first, Request(Operation::Connect)).m_result.Ok(), "first account connected");
		Check(send(second, Request(Operation::Connect, TestAccount(2))).m_result.Ok(), "second account connected");
		Check(1000000 == first->Snapshot().m_account.m_nBalance && 2000000 == second->Snapshot().m_account.m_nBalance, "session snapshots bound to account");
		std::atomic_int mismatchedReplies{ 0 };
		Check(!countertest::SendRequest(*first, Request(Operation::Funds, TestAccount(2)), [&mismatchedReplies](const CCounterReply&)
		{
			++mismatchedReplies;
		}).Ok() && 0 == mismatchedReplies.load(),
			  "account mismatch rejected without callback");
		Check(send(first, Request(Operation::Submit)).m_result.Ok(), "active order created");
		Check(!send(first, Request(Operation::Disconnect)).m_result.Ok(), "active order prevents business disconnect");
		Check(first->Snapshot().m_account.m_bConnected, "failed disconnect keeps session usable");
		CReqParam cancel = Request(Operation::Cancel);
		std::get<CCancelParam>(cancel.m_param).m_orderId = first->Snapshot().m_orders.front().m_orderId;
		Check(send(first, std::move(cancel)).m_result.Ok(), "cancel before disconnect");
		Check(send(first, Request(Operation::Disconnect)).m_result.Ok(), "backend confirms disconnect");
		first->Close();
		Check(!first->Snapshot().m_bReady && !countertest::SendRequest(*first, Request(Operation::Funds), {}).Ok(), "closed session rejects requests");
		Check(send(second, Request(Operation::Funds, TestAccount(2))).m_result.Ok(), "closing first leaves second running");

		std::promise<void> entered;
		std::promise<void> release;
		std::shared_future<void> released = release.get_future().share();
		Check(countertest::SendRequest(*second, Request(Operation::Funds, TestAccount(2)), [&entered, released](const CCounterReply&)
		{
			entered.set_value();
			released.wait_for(std::chrono::seconds(3));
		}).Ok(),
			  "block worker for deterministic retirement");
		Check(std::future_status::ready == entered.get_future().wait_for(std::chrono::seconds(3)), "worker barrier reached");
		std::promise<CCounterReply> retired;
		std::future<CCounterReply> retiredReply = retired.get_future();
		std::atomic_int completions{ 0 };
		Check(countertest::SendRequest(*second, Request(Operation::Funds, TestAccount(2)), [&retired, &completions](const CCounterReply& reply)
		{
			++completions;
			retired.set_value(reply);
		}).Ok(),
			  "old session request queued");
		std::shared_ptr<ITradeSession> replacement = dispatcher->Create({ TestAccount(2), "simulated" });
		release.set_value();
		Check(std::future_status::ready == retiredReply.wait_for(std::chrono::seconds(3)) && !retiredReply.get().m_result.Ok(), "queued old instance is rejected");
		Check(!countertest::SendRequest(*second, Request(Operation::Funds, TestAccount(2)), {}).Ok(), "retired wrapper cannot submit");
		Check(send(replacement, Request(Operation::Funds, TestAccount(2))).m_result.Ok(), "replacement session works");
		Check(1 == completions.load(), "retired request completes exactly once");
		std::atomic_int pending{ 0 };
		for (int index = 0; 64 > index; ++index)
		{
			Check(countertest::SendRequest(*replacement, Request(Operation::Funds, TestAccount(2)), [&pending](const CCounterReply&)
			{
				++pending;
			}).Ok(),
				  "shutdown request accepted");
		}
		dispatcher->Stop();
		Check(64 == pending.load(), "shutdown completes every accepted request");
		Check(nullptr == dispatcher->Create({ TestAccount(1), "simulated" }), "stopped dispatcher cannot create sessions");
		dispatcher.reset();
		Check(!replacement->Snapshot().m_bReady && !countertest::SendRequest(*replacement, Request(Operation::Funds, TestAccount(2)), {}).Ok(), "stopped dispatcher rejects requests");
	}

	void DemandSubscriptions()
	{
		std::unique_ptr<CSimulatedDispatcher> dispatcher = std::make_unique<CSimulatedDispatcher>();
		CSimulatedDispatcher& gateway = *dispatcher;
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		std::atomic_int64_t now{ 1100 };
		std::mutex subscriptionMutex;
		std::condition_variable subscriptionCondition;
		std::vector<std::pair<std::string, bool>> subscriptions;
		std::atomic_int subscriptionFailures{ 1 };
		// Stop the dispatcher before callback captures leave scope, including failed checks.
		struct CCounterStopGuard
		{
			CSimulatedDispatcher& m_counter;
			~CCounterStopGuard()
			{
				m_counter.Stop();
			}
		} cleanup{ gateway };
		Check(gateway.Start(std::make_unique<CStore>(memory), [&now]()
		{
			return now.load();
		}, { { TestAccount(1), 1000000, 0, false, true }, { TestAccount(2), 1000000, 0, false, true } }, { Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } })
				  .Ok(),
			  "demand gateway start");
		auto handler = [&](const std::string& security, bool subscribe)
		{
			if (subscribe && 0 < subscriptionFailures.load())
			{
				--subscriptionFailures;
				return false;
			}
			std::lock_guard<std::mutex> lock(subscriptionMutex);
			subscriptions.emplace_back(security, subscribe);
			subscriptionCondition.notify_all();
			return true;
		};
		gateway.SetDepthSubscriptionHandler(handler);
		gateway.OnMarketState(true);
		std::map<_TyAccount, std::shared_ptr<ITradeSession>> sessions;
		sessions.emplace(TestAccount(1), dispatcher->Create({ TestAccount(1), "simulated" }));
		sessions.emplace(TestAccount(2), dispatcher->Create({ TestAccount(2), "simulated" }));
		auto send = [&sessions](CReqParam request)
		{
			std::shared_ptr<std::promise<CCounterReply>> promise = std::make_shared<std::promise<CCounterReply>>();
			std::future<CCounterReply> future = promise->get_future();
			std::shared_ptr<ITradeSession> session = sessions.at(request.m_accountIndex);
			Check(countertest::SendRequest(*session, std::move(request), [promise](const CCounterReply& reply)
			{
				promise->set_value(reply);
			}).Ok(),
				  "demand request queued");
			Check(std::future_status::ready == future.wait_for(std::chrono::seconds(3)), "demand reply received");
			return future.get();
		};
		auto wait = [&](std::size_t count, bool subscribe)
		{
			std::unique_lock<std::mutex> lock(subscriptionMutex);
			Check(subscriptionCondition.wait_for(lock, std::chrono::seconds(3), [&]()
			{
				return subscriptions.size() >= count;
			}),
				  "subscription transition received");
			Check(count == subscriptions.size() && "600000.SSE" == subscriptions.back().first && subscribe == subscriptions.back().second, "expected demand transition");
		};
		Check(send(Request(Operation::Connect)).m_result.Ok(), "first account connected");
		Check(send(Request(Operation::Connect, TestAccount(2))).m_result.Ok(), "second account connected");
		{
			std::lock_guard<std::mutex> lock(subscriptionMutex);
			Check(subscriptions.empty(), "rules and connections do not subscribe");
		}
		Check(!send(Request(Operation::Submit, TestAccount(1), 1, 1)).m_result.Ok(), "rejected order does not subscribe");
		CCounterReply first = send(Request(Operation::Submit, TestAccount(1), 2));
		Check(first.m_result.Ok(), "first order waits for initial depth");
		wait(1, true);
		CCounterReply second = send(Request(Operation::Submit, TestAccount(2), 3));
		Check(second.m_result.Ok(), "second account shares subscription");
		CReqParam cancel = Request(Operation::Cancel);
		std::get<CCancelParam>(cancel.m_param).m_orderId = std::get<COperationReceipt>(first.m_data).m_orderId;
		Check(send(cancel).m_result.Ok(), "cancel first order");
		gateway.OnMarketState(false);
		gateway.OnMarketState(true);
		wait(2, true);
		cancel.m_accountIndex = TestAccount(2);
		std::get<CCancelParam>(cancel.m_param).m_orderId = std::get<COperationReceipt>(second.m_data).m_orderId;
		Check(send(cancel).m_result.Ok(), "cancel last order");
		wait(3, false);
		Check(send(Request(Operation::Submit, TestAccount(1), 4)).m_result.Ok(), "new order resubscribes");
		wait(4, true);
		gateway.OnDepth(CDepth{ "600000.SSE", 1, 1100, 2, {}, { { 1000, 50 } } });
		Check(send(Request(Operation::Funds)).m_result.Ok(), "partial fill barrier");
		Check(Status::PartiallyFilled == gateway.Snapshot(TestAccount(1)).m_orders.back().m_status, "partial fill retains demand");
		gateway.OnDepth(CDepth{ "600000.SSE", 2, 1100, 2, {}, { { 1000, 500 } } });
		wait(5, false);
		Check(send(Request(Operation::Submit, TestAccount(1), 5)).m_result.Ok(), "next order accepted");
		Check(Status::Accepted == gateway.Snapshot(TestAccount(1)).m_orders.back().m_status, "new demand cannot reuse unsubscribed cached depth");
		wait(6, true);
		gateway.Stop();
		wait(7, false);
		Check(gateway.Start(std::make_unique<CStore>(memory), [&now]()
		{
			return now.load();
		}, {}, { Rule(1) }, { { 1, 1000, 2000, 3000, 4000 } })
				  .Ok(),
			  "recover pending order");
		gateway.SetDepthSubscriptionHandler(handler);
		gateway.OnMarketState(true);
		wait(8, true);
		now = 4000;
		wait(9, false);
		gateway.Stop();
	}

	void IncrementalState()
	{
		CState state;
		state.m_accounts.emplace(TestAccount(1), CFunds{ TestAccount(1), 100000, 0, true, true });
		state.m_accounts.emplace(TestAccount(2), CFunds{ TestAccount(2), 200000, 0, true, true });
		std::string strBefore = EncodeState(state);
		{
			CStateEdit edit(state);
			edit.Save(&CState::m_accounts, TestAccount(1));
			state.m_accounts.at(TestAccount(1)).m_nBalance -= 1000;
			edit.Save(&CState::m_accounts, TestAccount(1));
			state.m_accounts.at(TestAccount(1)).m_nBalance -= 1000;
			edit.Save(&CState::m_orders, std::uint64_t{ 1 });
			state.m_orders.emplace(1, COrder{});
			state.m_nextOrder++;
			state.m_events.push_back(CCountEvent{});
			CState changes = edit.Changes();
			Check(1 == changes.m_accounts.size() && 1 == changes.m_orders.size() && changes.m_trades.empty() && 1 == changes.m_events.size(), "sparse delta excludes untouched history");
		}
		Check(strBefore == EncodeState(state), "journal restores existing and inserted rows and metadata");
		CAccountData cache;
		cache.m_account.m_accountIndex = TestAccount(1);
		CCountEvent event;
		event.m_accountIndex = TestAccount(1);
		event.m_funds = state.m_accounts.at(TestAccount(1));
		event.m_kind = EventTy::Order;
		event.m_order.m_orderId = 1;
		event.m_order.m_accountIndex = TestAccount(1);
		UpdateAccountData(cache, event);
		event.m_order.m_status = Status::Cancelled;
		UpdateAccountData(cache, event);
		Check(1 == cache.m_orders.size() && Status::Cancelled == cache.m_orders[0].m_status && TestAccount(1) == cache.m_account.m_accountIndex, "incremental cache replaces rows without duplicating history");
	}

	void Run()
	{
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		CSimulatedCore counter(std::make_unique<CStore>(memory));
		Check(counter.Start(0).Ok(), "start");
		Check(counter.AddAccount(TestAccount(1), 1000000).Ok() && counter.AddAccount(TestAccount(2), 1000000).Ok(), "migration");
		Check(counter.ImportRules({ Rule(1), Rule(2) }, { { 1, 1000, 2000, 3000, 4000 }, { 2, 5000, 6000, 7000, 8000 } }).Ok(), "rules");
		Connect(counter, TestAccount(1), 1100);
		Connect(counter, TestAccount(2), 1100);
		counter.OnMarketState(true);
		CCounterReply first = counter.Execute(Request(Operation::Submit, TestAccount(1), 10, 200), 1100);
		Check(first.m_result.Ok(), "submit");
		CCounterReply second = counter.Execute(Request(Operation::Submit, TestAccount(2), 11, 100, 1100), 1100);
		Check(second.m_result.Ok(), "submit2");
		CDepth depth{ "600000.SSE", 1, 1100, 2, { { 990, 500 } }, { { 1000, 150 } } };
		Check(counter.OnDepth(depth, 1100).Ok(), "depth");
		CAccountData snapshot = counter.Snapshot(TestAccount(1), 1100);
		CAccountData other = counter.Snapshot(TestAccount(2), 1100);
		Check(2 == snapshot.m_trades.size() + other.m_trades.size(), "partial fills");
		Check(100 == other.m_orders[0].m_nFilled && 50 == snapshot.m_orders[0].m_nFilled, "price priority across accounts");
		Check(!counter.OnDepth(depth, 1100).Ok() && 2 == counter.Snapshot(TestAccount(1), 1100).m_trades.size() + counter.Snapshot(TestAccount(2), 1100).m_trades.size(), "duplicate liquidity");
		Check(std::get<COperationReceipt>(counter.Execute(Request(Operation::Submit, TestAccount(1), 10, 200), 1100).m_data).m_orderId == std::get<COperationReceipt>(first.m_data).m_orderId, "idempotency");
		Check(!counter.Execute(Request(Operation::Submit, TestAccount(1), 10, 300), 1100).m_result.Ok(), "id conflict");
		CReqParam sell = Request(Operation::Submit, TestAccount(1), 12, 1);
		std::get<trade::CSubmitParam>(sell.m_param).m_order.m_side = Side::Sell;
		Check(!counter.Execute(sell, 1100).m_result.Ok(), "T+1");
		CReqParam cancel = Request(Operation::Cancel);
		std::get<trade::CCancelParam>(cancel.m_param).m_orderId = std::get<COperationReceipt>(first.m_data).m_orderId;
		Check(counter.Execute(cancel, 1200).m_result.Ok(), "cancel");
		Check(0 == counter.Snapshot(TestAccount(1), 1200).m_account.m_nFrozen, "release freeze");
		Check(counter.Advance(4100).Ok(), "daycut");
		Check(0 == counter.Snapshot(TestAccount(1), 4100).m_positions[0].m_nAvailable, "cannot sell before next trading day");
		std::uint64_t nVersion = counter.Snapshot(TestAccount(1), 4100).m_version;
		Check(counter.Advance(4200).Ok() && nVersion == counter.Snapshot(TestAccount(1), 4200).m_version, "daycut idempotency");
		std::get<trade::CSubmitParam>(sell.m_param).m_order.m_clientId = 13;
		sell.m_requestId = 13;
		Check(counter.Execute(sell, 5100).m_result.Ok(), "sell next day");
		depth.m_sequence = 2;
		depth.m_nTime = 5100;
		depth.m_bids[0].m_nPrice = 1000;
		Check(counter.OnDepth(depth, 5100).Ok(), "sell fill");
		Check(3 == counter.Snapshot(TestAccount(1), 5100).m_trades.size() + counter.Snapshot(TestAccount(2), 5100).m_trades.size(), "sell recorded");
		std::string strSaved = EncodeState(memory->m_state);
		CState restored;
		Check(DecodeState(strSaved, restored) && strSaved == EncodeState(restored), "codec");
		Check(!DecodeState(strSaved + " bad", restored), "bad codec");
		std::int64_t nCash = counter.Snapshot(TestAccount(1), 5100).m_account.m_nBalance;
		counter.Stop();
		CSimulatedCore restarted(std::make_unique<CStore>(memory));
		Check(restarted.Start(5200).Ok(), "restart");
		Check(nCash == restarted.Snapshot(TestAccount(1), 5200).m_account.m_nBalance, "cash persisted");
		Connect(restarted, TestAccount(1), 5200);
		restarted.OnMarketState(true);
		Check(restarted.Execute(Request(Operation::Submit, TestAccount(1), 14), 5200).m_result.Ok() && 3 == restarted.Snapshot(TestAccount(1), 5200).m_trades.size() + restarted.Snapshot(TestAccount(2), 5200).m_trades.size(), "restart requires fresh depth");
		memory->m_bFail = true;
		CReqParam failed = Request(Operation::Cancel);
		std::get<trade::CCancelParam>(failed.m_param).m_orderId = restarted.Snapshot(TestAccount(1), 5200).m_orders.back().m_orderId;
		std::uint64_t nTail = memory->m_state.m_sequence;
		std::string strBeforeFailure = EncodeState(memory->m_state);
		Check(!restarted.Execute(failed, 5200).m_result.Ok() && !restarted.Ready() && nTail == memory->m_state.m_sequence, "transaction failure fail closed");
		Check(strBeforeFailure == EncodeState(memory->m_state), "failed command preserves durable state");
		Check(restarted.Snapshot(TestAccount(1), 5200).m_account.m_nFrozen == memory->m_state.m_accounts.at(TestAccount(1)).m_nFrozen, "failed cancellation restores in-memory freeze");
		restarted.Stop();
		memory->m_bFail = false;
		CSimulatedCore replay(std::make_unique<CStore>(memory));
		Check(replay.Start(5300).Ok() && !replay.EventsAfter(nVersion).empty(), "replay");
		memory->m_bLease = false;
		Check(!replay.Renew().Ok() && !replay.Ready(), "fencing");
		replay.Stop();
	}

	void IdentityIsolation()
	{
		std::shared_ptr<CMemory> memory = std::make_shared<CMemory>();
		CSimulatedCore counter(std::make_unique<CStore>(memory));
		Check(counter.Start(0).Ok(), "identity core start");
		std::vector<_TyAccount> identities{ { 1, "000123" }, { 2, "000123" }, { 1, "123" } };
		std::int64_t balance = 10000;
		for (const auto& account : identities)
		{
			Check(counter.AddAccount(account, balance).Ok(), "distinct identity opened");
			Check(!counter.AddAccount(account, 1).Ok(), "duplicate opening rejected");
			balance += 10000;
		}
		std::string encoded = EncodeState(memory->m_state);
		CState restored;
		Check(DecodeState(encoded, restored) && 3 == restored.m_accounts.size(), "all identities survive persistence");
		Check(10000 == restored.m_accounts.at(identities[0]).m_nBalance && 30000 == restored.m_accounts.at(identities[2]).m_nBalance, "leading zero account remains distinct");
		_TyAccount special{ 9, "00: account \"" };
		_TyAccount parsed;
		Check(DecodeAccount(EncodeAccount(special), parsed) && special == parsed, "pair encoding preserves delimiters");
		Check(!DecodeAccount(EncodeAccount(special) + " trailing", parsed), "trailing identity data rejected");
		counter.Stop();
	}
} // namespace
int main()
{
	try
	{
		CReqParam request;
		Check(Operation::Funds == request.GetOperation(), "default funds query request");
		request.m_accountIndex = TestAccount(7);
		request.m_requestId = 99;
		request.m_param = CLoginParam{ "password" };
		CReqParam copied = request;
		request.m_param = CCancelParam{ 123 };
		Check(Operation::Login == copied.GetOperation() && "password" == std::get<CLoginParam>(copied.m_param).m_strPassword, "copied request owns login parameters");
		Check(Operation::Cancel == request.GetOperation() && 123 == std::get<CCancelParam>(request.m_param).m_orderId && TestAccount(7) == request.m_accountIndex && 99 == request.m_requestId, "payload determines operation while preserving identity");
		Check(nullptr == std::get_if<CLoginParam>(&request.m_param), "changing operation removes previous parameters");
		IdentityIsolation();
		IncrementalState();
		Run();
		Boundaries();
		DemandSubscriptions();
		SessionIsolation();
		DispatcherDestruction();
		std::cout << "counter tests passed\n";
		return 0;
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
}
