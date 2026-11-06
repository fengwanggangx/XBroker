#pragma once
#include "TradeTypes.h"
#include <functional>
#include <atomic>

namespace trade
{
	class ITradeSession
	{
	public:
		using _TyReplyHandler = std::function<void(const CCounterReply&)>;
		using _TyEventHandler = std::function<void(const CCountEvent&)>;
		ITradeSession() = default;
		virtual ~ITradeSession() = default;
		ITradeSession(const ITradeSession& arg) = delete;
		ITradeSession& operator=(const ITradeSession& arg) = delete;

		std::uint64_t GetId() const noexcept
		{
			return m_id;
		}

		// Register the response route once. Success means queued; deliver one reply per request.
		// Rejected submissions do not generate a reply.
		// Events may precede the reply; business acceptance is delivered in replies/events.
		virtual CResult SendRequest(CReqParam request) = 0;
		virtual void SetReplyHandler(_TyReplyHandler handler) = 0;
		virtual CAccountData Snapshot() const = 0;
		virtual void SetEventHandler(_TyEventHandler handler) = 0;
		// Resource cleanup only. Confirm business disconnect through SendRequest first.
		virtual void Close() = 0;

	private:
		inline static std::atomic_uint64_t m_nextId{ 1 };
		const std::uint64_t m_id{ m_nextId.fetch_add(1, std::memory_order_relaxed) };
	};
	class ICounterStore
	{
	public:
		virtual ~ICounterStore() = default;
		virtual CResult Acquire(CState& state) = 0;
		// Commit persists a sparse change set plus final metadata, fenced by the previous event sequence.
		virtual CResult Commit(const CState& state, std::uint64_t nExpectedSequence) = 0;
		virtual CResult Renew() = 0;
		virtual void Release() = 0;
	};
	class IMarketInput
	{
	public:
		virtual ~IMarketInput() = default;
		virtual void OnDepth(CDepth depth) = 0;
		virtual void OnMarketState(bool bAvailable) = 0;
	};
} // namespace trade
