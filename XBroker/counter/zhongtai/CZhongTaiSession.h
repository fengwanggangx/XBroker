#pragma once
#include "ZhongTaiTypes.h"
#include <mutex>
class CZhongTaiDispatcher;

namespace trade::zhongtai
{
	class CZhongTaiSession final : public ITradeSession
	{
	public:
		CZhongTaiSession(CZhongTaiDispatcher* dispatcher, const _TyAccount& account, std::uint64_t instance);
		~CZhongTaiSession() override;
		void Close() override;
		void SetEventHandler(_TyEventHandler handler) override;
		void SetReplyHandler(_TyReplyHandler handler) override;
		CResult SendRequest(CReqParam request) override;
		CAccountData Snapshot() const override;
		void Publish(const CCountEvent& event);
		void SetSnapshot(const CAccountData& snapshot);

	private:
		CZhongTaiDispatcher* m_pDispatcher; // Borrowed; the dispatcher closes sessions before destruction.
		_TyAccount m_account;
		std::uint64_t m_instance;
		mutable std::mutex m_mtx_state;
		CAccountData m_snapshot;
		_TyEventHandler m_handler;
		_TyReplyHandler m_handler_reply;
		bool m_stopped{ false };
	};
} // namespace trade::zhongtai
