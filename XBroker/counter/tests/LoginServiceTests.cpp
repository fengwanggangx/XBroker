#include "../../service/CTradeService.h"
#include <deque>
#include <iostream>
#include <stdexcept>

class CLoginServiceTests
{
	class CFake final : public trade::ITradeSession
	{
	public:
		trade::CResult SendRequest(trade::CReqParam request, _TyReplyHandler handler) override
		{
			m_pending.emplace_back(std::move(request), std::move(handler));
			return {};
		}

		void SetEventHandler(_TyEventHandler handler) override
		{
			m_events = std::move(handler);
		}

		trade::CAccountData Snapshot() const override
		{
			return {};
		}

		void Complete(bool success)
		{
			std::pair<trade::CReqParam, _TyReplyHandler> work = std::move(m_pending.front());
			m_pending.pop_front();
			trade::CCounterReply reply;
			reply.m_accountIndex = work.first.m_accountIndex;
			reply.m_operation = work.first.GetOperation();
			reply.m_data = trade::COperationReceipt{};
			if (!success)
			{
				reply.m_result = { trade::Error::Rejected, "credentials rejected" };
			}
			work.second(reply);
		}

		void Emit(const _TyAccount& account, bool connected)
		{
			trade::CCountEvent event;
			event.m_accountIndex = account;
			event.m_funds.m_bConnected = connected;
			m_events(event);
		}

		bool IsClosed() const
		{
			return m_closed;
		}

		void Close() override
		{
			m_closed = true;
			while (!m_pending.empty())
			{
				Complete(false);
			}
		}

	private:
		bool m_closed{ false };
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
		WriteAccountIdentity(account, login);
		login.SetExtraData("password", "secret");
		login.SetExtraData("site", "default");
		Check(service.HandleAccountRequest(login), "first login queued");
		Check(!service.HandleAccountRequest(login), "concurrent duplicate rejected");
		Check(service.m_accounts.empty() && 1 == sessions.size(), "pending login not published");
		sessions.back()->Complete(false);
		Check(service.m_accounts.empty() && service.m_logging.empty(), "failed login not published");
		Check(service.HandleAccountRequest(login), "retry creates new session");
		sessions.back()->Complete(true);
		Check(1 == service.m_accounts.size() && TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "successful login published");
		service.HandleAccountRequest(login);
		Check(2 == sessions.size(), "connected duplicate reuses session");
		sessions.back()->Emit(account, false);
		Check(service.HandleAccountRequest(login), "disconnected account can login");
		sessions.back()->Complete(true);
		sessions[1]->Emit(account, false);
		Check(TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "old session event ignored");
		CRequest disconnect;
		disconnect.SetType(_TyRequest::TRADE_ACCOUNT_DISCONNECT);
		WriteAccountIdentity(account, disconnect);
		Check(service.HandleAccountRequest(disconnect), "disconnect queued");
		sessions.back()->Complete(false);
		Check(!sessions.back()->IsClosed() && TradeAccountState::Connected == service.m_accounts.at(account).m_info.m_state, "failed disconnect preserves session");
		Check(service.HandleAccountRequest(disconnect), "disconnect retry queued");
		sessions.back()->Complete(true);
		Check(sessions.back()->IsClosed() && TradeAccountState::Disconnected == service.m_accounts.at(account).m_info.m_state, "confirmed disconnect closes session");
		Check(service.HandleAccountRequest(login), "explicitly closed account can login again");
		sessions.back()->Complete(true);
		Check(!sessions.back()->IsClosed(), "relogin uses a fresh session");
		service.Stop();
	}
};

int main()
{
	try
	{
		CLoginServiceTests::Run();
		std::cout << "dynamic login service tests passed\n";
		return 0;
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
}
