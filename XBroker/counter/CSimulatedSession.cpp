#include "CSimulatedSession.h"
#include "CSimulatedDispatcher.h"

CSimulatedSession::CSimulatedSession(CSimulatedDispatcher* dispatcher, const _TyAccount& account, std::uint64_t instance) : m_dispatcher(dispatcher), m_account(account), m_instance(instance)
{
}

CSimulatedSession::~CSimulatedSession()
{
	Close();
}

void CSimulatedSession::Close()
{
	std::lock_guard<std::mutex> lock(m_state_mutex);
	m_closed = true;
	m_dispatcher = nullptr;
	m_handler = {};
	m_handler_reply = {};
}

void CSimulatedSession::SetReplyHandler(_TyReplyHandler handler)
{
	std::lock_guard<std::mutex> lock(m_state_mutex);
	if (!m_closed)
	{
		m_handler_reply = std::move(handler);
	}
}

void CSimulatedSession::SetEventHandler(_TyEventHandler handler)
{
	std::lock_guard<std::mutex> lock(m_state_mutex);
	if (!m_closed)
	{
		m_handler = std::move(handler);
	}
}

trade::CAccountData CSimulatedSession::Snapshot() const
{
	std::lock_guard<std::mutex> lock(m_state_mutex);
	if (m_closed)
	{
		return {};
	}
	return m_dispatcher->IsCurrent(m_account, m_instance) ? m_dispatcher->Snapshot(m_account) : trade::CAccountData{};
}

trade::CResult CSimulatedSession::SendRequest(trade::CReqParam request)
{
	std::lock_guard<std::mutex> lock(m_state_mutex);
	if (m_closed || m_account != request.m_account_index)
	{
		return { trade::Error::Rejected, "closed session or account mismatch" };
	}
	return m_dispatcher->Send(std::move(request), m_handler_reply, m_instance);
}

void CSimulatedSession::Publish(const trade::CCountEvent& event)
{
	_TyEventHandler handler;
	{
		std::lock_guard<std::mutex> lock(m_state_mutex);
		if (m_closed)
		{
			return;
		}
		handler = m_handler;
	}
	if (nullptr != handler)
	{
		trade::CCountEvent sourceEvent = event;
		sourceEvent.m_session_id = GetId();
		handler(sourceEvent);
	}
}

std::optional<trade::CExecutionRule> CSimulatedSession::GetExecutionRules(const std::string& security) const
{
	std::lock_guard<std::mutex> lock(m_state_mutex);
	return m_closed ? std::nullopt : m_dispatcher->GetExecutionRules(security);
}
