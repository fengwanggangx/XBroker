#pragma once
#include "ITradeSession.h"
#include <memory>
#include <mutex>

class CSimulatedDispatcher;

class CSimulatedSession final : public trade::ITradeSession
{
public:
	CSimulatedSession(CSimulatedDispatcher* dispatcher, const _TyAccount& account, std::uint64_t instance);
	~CSimulatedSession() override;
	void Close() override;
	trade::CAccountData Snapshot() const override;
	void SetEventHandler(_TyEventHandler handler) override;
	void SetReplyHandler(_TyReplyHandler handler) override;
	trade::CResult SendRequest(trade::CReqParam request) override;
	void Publish(const trade::CCountEvent& event);

private:
	CSimulatedDispatcher* m_pDispatcher; // Borrowed; the dispatcher closes sessions before destruction.
	_TyAccount m_account;
	std::uint64_t m_instance;
	mutable std::mutex m_mtx_state;
	_TyEventHandler m_handler;
	_TyReplyHandler m_handler_reply;
	bool m_closed{ false };
};
