#include "../../service/CCounterFactory.h"
#include <deque>
#include <iostream>
#include <stdexcept>

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

class CDeferredBroker final : public trade::ITradeSession
{
public:
	trade::CResult SendRequest(trade::CReqParam request, _TyReplyHandler handler) override
	{
		m_work.emplace_back([this, request = std::move(request), handler = std::move(handler)]()
		{
			trade::CCountEvent event;
			event.m_accountIndex = request.m_accountIndex;
			event.m_requestId = request.m_requestId;
			event.m_kind = trade::Operation::Submit == request.GetOperation() ? trade::EventTy::Order : trade::EventTy::Session;
			if (const trade::CSubmitParam* params = std::get_if<trade::CSubmitParam>(&request.m_param); nullptr != params)
			{
				event.m_order = params->m_order;
			}
			event.m_order.m_orderId = 123;
			event.m_funds = { request.m_accountIndex, 10000, 0, true, true };
			m_handler(event);
			trade::CCounterReply reply;
			reply.m_requestId = request.m_requestId;
			reply.m_accountIndex = request.m_accountIndex;
			reply.m_operation = request.GetOperation();
			reply.m_data = trade::COperationReceipt{ event.m_order.m_orderId };
			handler(reply);
		});
		return {};
	}

	void Drain()
	{
		while (!m_work.empty())
		{
			std::function<void()> work = std::move(m_work.front());
			m_work.pop_front();
			work();
		}
	}
	void SetEventHandler(_TyEventHandler handler) override
	{
		m_handler = std::move(handler);
	}
	trade::CAccountData Snapshot() const override
	{
		return {};
	}
	void Close() override
	{
		Drain();
	}

private:
	std::deque<std::function<void()>> m_work;
	_TyEventHandler m_handler;
};
int main()
{
	trade::CCounterFactory registry;
	int nStops = 0;
	std::shared_ptr<CDeferredBroker> adapter = std::make_shared<CDeferredBroker>();
	if (!registry.Register("fake", [adapter](const trade::CCounterConfig&)
	{
		return adapter;
	}, [&nStops]()
	{
		++nStops;
	}) ||
		nullptr != registry.Create({ TestAccount(1), "missing" }))
	{
		return 1;
	}
	std::shared_ptr<trade::ITradeSession> gateway = registry.Create({ TestAccount(7), "fake" });
	int nEvents = 0;
	int nReplies = 0;
	gateway->SetEventHandler([&nEvents](const trade::CCountEvent& event)
	{
		if (TestAccount(7) == event.m_accountIndex)
		{
			++nEvents;
		}
	});
	for (trade::Operation operation : { trade::Operation::Login, trade::Operation::Submit })
	{
		trade::CReqParam request;
		request.m_accountIndex = TestAccount(7);
		request.m_param = MakeParams(operation);
		if (!gateway->SendRequest(std::move(request), [&nReplies](const trade::CCounterReply& reply)
		{
			if (reply.m_result.Ok())
			{
				++nReplies;
			}
		}).Ok())
		{
			return 2;
		}
	}
	if (0 != nReplies || 0 != nEvents)
	{
		return 3;
	}
	adapter->Drain();
	if (2 != nReplies || 2 != nEvents)
	{
		return 4;
	}
	gateway->Close();
	registry.Stop();
	registry.Stop();
	if (1 != nStops || registry.Contains("fake") || nullptr != registry.Create({ TestAccount(7), "fake" }))
	{
		return 5;
	}
	std::cout << "adapter contract passed\n";
}
