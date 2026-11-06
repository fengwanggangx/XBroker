#include "CSimulatedSession.h"
#include "CSimulatedDispatcher.h"

CSimulatedSession::CSimulatedSession(CSimulatedDispatcher* dispatcher, const _TyAccount& account, std::uint64_t instance) : m_pDispatcher(dispatcher), m_account(account), m_instance(instance)
{
}

CSimulatedSession::~CSimulatedSession()
{
	Close();
}

void CSimulatedSession::Close()
{
	std::lock_guard<std::mutex> lock(m_mtx_state);
	m_closed = true;
	m_pDispatcher = nullptr;
	m_handler = {};
	m_handler_reply = {};
}

void CSimulatedSession::SetReplyHandler(_TyReplyHandler handler)
{
	std::lock_guard<std::mutex> lock(m_mtx_state);
	if (!m_closed)
	{
		m_handler_reply = std::move(handler);
	}
}

void CSimulatedSession::SetEventHandler(_TyEventHandler handler)
{
	std::lock_guard<std::mutex> lock(m_mtx_state);
	if (!m_closed)
	{
		m_handler = std::move(handler);
	}
}

trade::CAccountData CSimulatedSession::Snapshot() const
{
	std::lock_guard<std::mutex> lock(m_mtx_state);
	if (m_closed)
	{
		return {};
	}
	return m_pDispatcher->IsCurrent(m_account, m_instance) ? m_pDispatcher->Snapshot(m_account) : trade::CAccountData{};
}

trade::CResult CSimulatedSession::SendRequest(trade::CReqParam request)
{
	std::lock_guard<std::mutex> lock(m_mtx_state);
	if (m_closed || m_account != request.m_accountIndex)
	{
		return { trade::Error::Rejected, "closed session or account mismatch" };
	}
	return m_pDispatcher->Send(std::move(request), m_handler_reply, m_instance);
}

void CSimulatedSession::Publish(const trade::CCountEvent& event)
{
	_TyEventHandler handler;
	{
		std::lock_guard<std::mutex> lock(m_mtx_state);
		if (m_closed)
		{
			return;
		}
		handler = m_handler;
	}
	if (nullptr != handler)
	{
		trade::CCountEvent sourceEvent = event;
		sourceEvent.m_sessionId = GetId();
		handler(sourceEvent);
	}
}
