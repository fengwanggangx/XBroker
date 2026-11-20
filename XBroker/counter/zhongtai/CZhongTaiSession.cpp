#include "CZhongTaiSession.h"
#include "CZhongTaiDispatcher.h"
namespace trade::zhongtai
{
	CZhongTaiSession::CZhongTaiSession(CZhongTaiDispatcher* dispatcher, const _TyAccount& account, std::uint64_t instance) : m_dispatcher(dispatcher), m_account(account), m_instance(instance)
	{
		m_snapshot.m_account.m_account_index = account;
		m_snapshot.m_account.m_funds_valid = false;
	}
	CZhongTaiSession::~CZhongTaiSession()
	{
		Close();
	}

	void CZhongTaiSession::Close()
	{
		{
			std::lock_guard<std::mutex> lock(m_state_mutex);
			if (m_stopped)
			{
				return;
			}
			m_stopped = true;
			m_dispatcher = nullptr;
			m_handler = {};
			m_handler_reply = {};
			m_snapshot.m_ready = false;
		}
	}

	void CZhongTaiSession::SetReplyHandler(_TyReplyHandler handler)
	{
		std::lock_guard<std::mutex> lock(m_state_mutex);
		if (!m_stopped)
		{
			m_handler_reply = std::move(handler);
		}
	}

	void CZhongTaiSession::SetEventHandler(_TyEventHandler handler)
	{
		std::lock_guard<std::mutex> lock(m_state_mutex);
		if (!m_stopped)
		{
			m_handler = std::move(handler);
		}
	}

	CResult CZhongTaiSession::SendRequest(CReqParam request)
	{
		std::lock_guard<std::mutex> lock(m_state_mutex);
		if (m_stopped || m_account != request.m_account_index)
		{
			return { Error::Rejected, "stopped counter or account mismatch" };
		}
		return m_dispatcher->Send(std::move(request), m_handler_reply, m_instance);
	}

	CAccountData CZhongTaiSession::Snapshot() const
	{
		std::lock_guard<std::mutex> lock(m_state_mutex);
		return m_snapshot;
	}

	void CZhongTaiSession::SetSnapshot(const CAccountData& snapshot)
	{
		std::lock_guard<std::mutex> lock(m_state_mutex);
		if (!m_stopped)
		{
			m_snapshot = snapshot;
		}
	}

	void CZhongTaiSession::Publish(const CCountEvent& event)
	{
		_TyEventHandler handler;
		{
			std::lock_guard<std::mutex> lock(m_state_mutex);
			if (m_stopped)
			{
				return;
			}
			handler = m_handler;
		}
		if (nullptr != handler)
		{
			try
			{
				CCountEvent sourceEvent = event;
				sourceEvent.m_session_id = GetId();
				handler(sourceEvent);
			}
			catch (...)
			{
			}
		}
	}
} // namespace trade::zhongtai
