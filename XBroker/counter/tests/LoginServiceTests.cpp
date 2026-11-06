#include "../../service/CTradeService.h"
#include <deque>
#include <iostream>
#include <stdexcept>
#include <future>
#include <atomic>

class CLoginServiceTests
{
	class CFake final : public trade::ITradeSession
	{
	public:
		trade::CResult SendRequest(trade::CReqParam request) override
		{
			if (m_reject)
			{
				m_reject = false;
				return { trade::Error::Rejected, "queue rejected" };
			}
			m_pending.emplace_back(std::move(request), m_handler_reply);
			if (m_immediate)
			{
				Complete(true);
			}
			return {};
		}

		void SetReplyHandler(_TyReplyHandler handler) override
		{
			m_handler_reply = std::move(handler);
			++m_replyRegistrations;
		}
		void SetEventHandler(_TyEventHandler handler) override
		{
			m_events = std::move(handler);
		}

		trade::CAccountData Snapshot() const override
		{
			return {};
		}

		trade::CCounterReply Complete(bool success, std::size_t index = 0)
		{
			std::pair<trade::CReqParam, _TyReplyHandler> work = std::move(m_pending.at(index));
			m_pending.erase(m_pending.begin() + index);
			trade::CCounterReply reply;
			reply.m_accountIndex = work.first.m_accountIndex;
			reply.m_requestId = work.first.m_requestId;
			reply.m_correlationId = work.first.m_correlationId;
			reply.m_operation = work.first.GetOperation();
			reply.m_data = trade::COperationReceipt{};
			if (trade::Operation::Funds == reply.m_operation)
			{
				reply.m_data = trade::CFunds{};
			}
			if (!success)
			{
				reply.m_result = { trade::Error::Rejected, "credentials rejected" };
			}
			work.second(reply);
			return reply;
		}

		void Emit(const _TyAccount& account, bool connected)
		{
			trade::CCountEvent event;
			event.m_sessionId = GetId();
			event.m_accountIndex = account;
			event.m_funds.m_bConnected = connected;
			m_events(event);
		}

		std::size_t GetReplyRegistrationCount() const
		{
			return m_replyRegistrations;
		}

		void RejectNext()
		{
			m_reject = true;
		}

		void SetImmediate(bool immediate)
		{
			m_immediate = immediate;
		}

		void DropOnClose()
		{
			m_dropOnClose = true;
		}

		bool IsClosed() const
		{
			return m_closed;
		}

		void Close() override
		{
			m_closed = true;
			if (m_dropOnClose)
			{
				m_pending.clear();
			}
			while (!m_pending.empty())
			{
				Complete(false);
			}
		}

	private:
		_TyReplyHandler m_handler_reply;
		std::size_t m_replyRegistrations{ 0 };
		bool m_closed{ false };
		bool m_reject{ false };
		bool m_immediate{ false };
		bool m_dropOnClose{ false };
		std::deque<std::pair<trade::CReqParam, _TyReplyHandler>> m_pending;
		_TyEventHandler m_events;
	};

	static void Check(bool value, const char* message)
	{
		if (!value)
		{
			throw std::runtime_error(message);
		}
	}

public:
	static void Run()
	{
		CTradeService service;
		// Exercise deadlines deterministically without racing the response assertions.
		service.m_pendingThread.request_stop();
		service.m_condition_pending.notify_all();
		service.m_pendingThread.join();
		service.SetCounterReplyHandler(std::bind_front(&CTradeService::OnCounterReply, &service));
		service.SetCounterEventHandler(std::bind_front(&CTradeService::OnCounterEvent, &service));
		std::vector<CRequest> responses;
		std::vector<net::_TyConnectionId> connections;
		service.SetReplyHandler([&responses, &connections](net::_TyConnectionId connection, const CRequest& response)
		{
			responses.emplace_back(response);
			connections.emplace_back(connection);
			return true;
		});
		CBrokerInfo broker;
		broker.m_strAdapter = "fake";
		broker.m_brokerId = 17;
		broker.m_strName = "Broker";
		broker.m_sites.emplace("default");
		service.m_brokers.emplace(17, std::move(broker));
		std::vector<std::shared_ptr<CFake>> sessions;
		service.CounterFactory().Register("fake", [&sessions](const trade::CCounterConfig&)
		{
			std::shared_ptr<CFake> counter = std::make_shared<CFake>();
			sessions.emplace_back(counter);
			return counter;
		});
		_TyAccount account{ 17, "000123" };
		CRequest login;
		login.SetType(_TyRequest::TRADE_ACCOUNT_LOGIN);
		login.SetId(901);
		login.SetConnectionId(7070);
		WriteAccountIdentity(account, login);
		login.SetExtraData("password", "secret");
		login.SetExtraData("site", "default");
		Check(service.OnRequest(login), "first login queued");
		Check(!service.OnRequest(login), "concurrent duplicate rejected");
		Check(service.m_accounts.empty() && 1 == sessions.size(), "pending login not published");
		sessions.back()->Complete(false);
		Check(2 == responses.size() && 7070 == connections.back() && 901 == responses.back().GetId() && _TyRequest::TRADE_ACCOUNT_LOGIN == responses.back().GetType() && responses.back().GetErrorInfo().has_value(), "async reply preserves client connection, request id and failure");
		Check(service.m_accounts.empty() && service.m_logging.empty(), "failed login not published");
		Check(service.OnRequest(login), "retry creates new session");
		sessions.back()->Complete(true);
		Check(1 == service.m_accounts.size() && TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "successful login published");
		service.OnRequest(login);
		Check(2 == sessions.size(), "connected duplicate reuses session");
		Check(0 != sessions.back()->GetId() && sessions.front()->GetId() != sessions.back()->GetId(), "each session has an immutable unique id");
		trade::CCountEvent stale;
		stale.m_accountIndex = account;
		stale.m_sessionId = sessions.front()->GetId();
		service.OnCounterEvent(stale);
		Check(TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "old session id rejected for the same account");
		stale.m_sessionId = 0;
		service.OnCounterEvent(stale);
		Check(TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "missing session id rejected");
		sessions.back()->Emit({ 99, "other" }, false);
		Check(TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "event account must match its bound source account");
		CRequest funds;
		funds.SetType(_TyRequest::TRADE_FUNDS_QUERY);
		WriteAccountIdentity(account, funds);
		funds.SetId(1001);
		funds.SetConnectionId(101);
		Check(service.OnRequest(funds), "first client funds query queued");
		funds.SetConnectionId(202);
		Check(service.OnRequest(funds), "same client request id on another connection queued");
		trade::CCounterReply second = sessions.back()->Complete(true, 1);
		Check(202 == connections.back() && 1001 == responses.back().GetId(), "out-of-order reply returns to second client");
		trade::CCounterReply first = sessions.back()->Complete(true);
		Check(101 == connections.back() && 1001 == responses.back().GetId(), "first asynchronous reply returns to first client");
		Check(first.m_correlationId != second.m_correlationId && 1001 == first.m_requestId && 1001 == second.m_requestId, "internal correlation is unique without changing business request ids");
		std::size_t completedCount = responses.size();
		service.OnCounterReply(first);
		Check(completedCount == responses.size() && service.m_pending.empty(), "duplicate reply ignored");
		Check(service.OnRequest(funds), "timeout query queued");
		{
			std::lock_guard<std::mutex> lock(service.m_mtx_pending);
			service.m_pending.begin()->second.m_deadline = std::chrono::steady_clock::time_point::min();
		}
		service.ExpirePendingRequests();
		Check(completedCount + 1 == responses.size() && responses.back().GetErrorInfo().has_value() && service.m_pending.empty(), "timeout completes and removes pending query");
		sessions.back()->Complete(true);
		Check(completedCount + 1 == responses.size(), "late reply ignored after timeout");
		sessions.back()->RejectNext();
		Check(!service.OnRequest(funds) && service.m_pending.empty(), "enqueue rejection clears pending immediately");
		sessions.back()->SetImmediate(true);
		Check(service.OnRequest(funds) && service.m_pending.empty() && !responses.back().GetErrorInfo().has_value(), "synchronous reply sees pending before send");
		sessions.back()->SetImmediate(false);
		Check(1 == sessions.back()->GetReplyRegistrationCount(), "response route registered once for repeated requests");
		Check(service.OnRequest(funds), "metadata validation query queued");
		trade::CCounterReply mismatched;
		{
			std::lock_guard<std::mutex> lock(service.m_mtx_pending);
			mismatched.m_correlationId = service.m_pending.begin()->first;
		}
		mismatched.m_accountIndex = { 99, "other" };
		mismatched.m_operation = trade::Operation::Funds;
		completedCount = responses.size();
		service.OnCounterReply(mismatched);
		Check(1 == service.m_pending.size() && completedCount == responses.size(), "wrong account cannot complete pending request");
		mismatched.m_accountIndex = account;
		mismatched.m_operation = trade::Operation::Cancel;
		service.OnCounterReply(mismatched);
		Check(1 == service.m_pending.size(), "wrong operation cannot complete pending request");
		sessions.back()->Complete(true);
		Check(service.m_pending.empty(), "valid reply completes after malformed replies");
		sessions.back()->Emit(account, false);
		Check(service.OnRequest(login), "disconnected account can login");
		sessions.back()->Complete(true);
		sessions[1]->Emit(account, false);
		Check(TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "old session event ignored");
		CRequest disconnect;
		disconnect.SetType(_TyRequest::TRADE_ACCOUNT_DISCONNECT);
		WriteAccountIdentity(account, disconnect);
		Check(service.OnRequest(disconnect), "disconnect queued");
		sessions.back()->Complete(false);
		Check(!sessions.back()->IsClosed() && TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "failed disconnect preserves session");
		Check(service.OnRequest(disconnect), "disconnect retry queued");
		sessions.back()->Complete(true);
		Check(sessions.back()->IsClosed() && TradeAccountState::Disconnected == service.m_accounts.at(account).m_info.m_state, "confirmed disconnect closes session");
		Check(service.OnRequest(login), "explicitly closed account can login again");
		sessions.back()->Complete(true);
		Check(!sessions.back()->IsClosed(), "relogin uses a fresh session");
		sessions.back()->Emit(account, false);
		Check(service.OnRequest(login), "login timeout fixture queued");
		{
			std::lock_guard<std::mutex> lock(service.m_mtx_pending);
			service.m_pending.begin()->second.m_deadline = std::chrono::steady_clock::time_point::min();
		}
		service.ExpirePendingRequests();
		completedCount = responses.size();
		Check(service.m_logging.empty() && service.m_pending.empty() && TradeAccountState::Error == service.m_accounts.at(account).m_info.m_state, "login timeout releases login slot");
		sessions.back()->Complete(true);
		Check(completedCount == responses.size() && TradeAccountState::Error == service.m_accounts.at(account).m_info.m_state, "late successful login does not publish timed-out session");
		Check(service.OnRequest(login), "login retry after timeout queued");
		sessions.back()->Complete(true);
		Check(service.OnRequest(funds), "shutdown query queued");
		completedCount = responses.size();
		sessions.back()->DropOnClose();
		service.Stop();
		Check(service.m_pending.empty() && completedCount + 1 == responses.size() && responses.back().GetErrorInfo().has_value(), "shutdown fails requests whose adapter drops replies");
	}
	static void CheckTimer()
	{
		std::promise<CRequest> completion;
		std::future<CRequest> future = completion.get_future();
		std::atomic_int replies{ 0 };
		CTradeService service;
		service.SetCounterReplyHandler(std::bind_front(&CTradeService::OnCounterReply, &service));
		service.SetReplyHandler([&completion, &replies](net::_TyConnectionId, const CRequest& response)
		{
			if (0 == replies.fetch_add(1))
			{
				completion.set_value(response);
			}
			return true;
		});
		_TyAccount account{ 17, "000123" };
		std::shared_ptr<CFake> counter = std::make_shared<CFake>();
		counter->SetReplyHandler(std::bind_front(&CTradeService::OnCounterReply, &service));
		CTradeAccountInfo info = AccountInfo(account);
		info.m_state = TradeAccountState::Connected;
		service.m_accounts.emplace(account, CTradeAccount{ info, counter });
		CRequest request;
		request.SetType(_TyRequest::TRADE_FUNDS_QUERY);
		request.SetId(8080);
		request.SetConnectionId(123);
		WriteAccountIdentity(account, request);
		Check(service.OnRequest(request), "real timeout fixture queued");
		{
			std::lock_guard<std::mutex> lock(service.m_mtx_pending);
			service.m_pending.begin()->second.m_deadline = std::chrono::steady_clock::time_point::min();
		}
		Check(std::future_status::ready == future.wait_for(std::chrono::seconds(3)), "maintenance thread completes expired request");
		CRequest response = future.get();
		Check(8080 == response.GetId() && response.GetErrorInfo().has_value(), "timer reply retains original id");
		counter->Complete(true);
		service.Stop();
		Check(1 == replies.load(), "timer, late reply and stop complete only once");
	}
};

int main()
{
	try
	{
		CLoginServiceTests::Run();
		CLoginServiceTests::CheckTimer();
		std::cout << "dynamic login service tests passed\n";
		return 0;
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
}
