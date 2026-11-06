#include "CZhongTaiSession.h"
#include "CZhongTaiDispatcher.h"
namespace trade::zhongtai
{
	CZhongTaiSession::CZhongTaiSession(CZhongTaiDispatcher* dispatcher, const _TyAccount& account, std::uint64_t instance) : m_pDispatcher(dispatcher), m_account(account), m_instance(instance)
	{
		m_snapshot.m_account.m_accountIndex = account;
		m_snapshot.m_account.m_bFundsValid = false;
	}
	CZhongTaiSession::~CZhongTaiSession()
	{
		Close();
	}

	void CZhongTaiSession::Close()
	{
		{
			std::lock_guard<std::mutex> lock(m_mtx_state);
			if (m_stopped)
			{
				return;
			}
			m_stopped = true;
			m_pDispatcher = nullptr;
			m_handler = {};
			m_handler_reply = {};
			m_snapshot.m_bReady = false;
		}
	}

	void CZhongTaiSession::SetReplyHandler(_TyReplyHandler handler)
	{
		std::lock_guard<std::mutex> lock(m_mtx_state);
		if (!m_stopped)
		{
			m_handler_reply = std::move(handler);
		}
	}

	void CZhongTaiSession::SetEventHandler(_TyEventHandler handler)
	{
		std::lock_guard<std::mutex> lock(m_mtx_state);
		if (!m_stopped)
		{
			m_handler = std::move(handler);
		}
	}

	CResult CZhongTaiSession::SendRequest(CReqParam request)
	{
		std::lock_guard<std::mutex> lock(m_mtx_state);
		if (m_stopped || m_account != request.m_accountIndex)
		{
			return { Error::Rejected, "stopped counter or account mismatch" };
		}
		return m_pDispatcher->Send(std::move(request), m_handler_reply, m_instance);
	}

	CAccountData CZhongTaiSession::Snapshot() const
	{
		std::lock_guard<std::mutex> lock(m_mtx_state);
		return m_snapshot;
	}

	void CZhongTaiSession::SetSnapshot(const CAccountData& snapshot)
	{
		std::lock_guard<std::mutex> lock(m_mtx_state);
		if (!m_stopped)
		{
			m_snapshot = snapshot;
		}
	}

	void CZhongTaiSession::Publish(const CCountEvent& event)
	{
		_TyEventHandler handler;
		{
			std::lock_guard<std::mutex> lock(m_mtx_state);
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
				sourceEvent.m_sessionId = GetId();
				handler(sourceEvent);
			}
			catch (...)
			{
			}
		}
	}
} // namespace trade::zhongtai
