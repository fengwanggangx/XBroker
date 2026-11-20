#pragma once
#include "ITradeSession.h"
#include "../database/IDataBase.h"

namespace trade
{
	class CStorager final : public ICounterStore
	{
	public:
		explicit CStorager(std::string strOwner);
		CResult Acquire(CState& state) override;
		CResult Commit(const CState& state, std::uint64_t nExpectedSequence) override;
		CResult Renew() override;
		void Release() override;

	private:
		bool CheckFence(db::IDataBase& session, bool bAcquire);
		std::string m_owner;
		std::uint64_t m_fence{ 0 };
	};
} // namespace trade
