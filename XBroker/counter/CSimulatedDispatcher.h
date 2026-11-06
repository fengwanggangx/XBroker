#pragma once
#include "CSimulatedCore.h"
#include "CounterConfig.h"
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <atomic>
#include <set>

class CRequest;
enum class SessionState;

class CSimulatedDispatcher final : public trade::IMarketInput
{
public:
	using _TyClock = std::function<std::int64_t()>;
	using _TyDepthSubscriptionHandler = std::function<bool(const std::string&, bool)>;

	CSimulatedDispatcher() = default;
	~CSimulatedDispatcher() override;
	using _TyReplyHandler = trade::ITradeSession::_TyReplyHandler;
	std::shared_ptr<trade::ITradeSession> Create(const trade::CCounterConfig& config);
	bool IsCurrent(const _TyAccount& account, std::uint64_t instance) const;

	void Stop();
	trade::CResult Initialize();
	trade::CResult Start(const std::vector<trade::CFunds>& accounts, const std::vector<trade::CRule>& rules, const std::vector<trade::CCalendar>& calendar);
	trade::CResult Start(std::unique_ptr<trade::ICounterStore> store, _TyClock&& clock, const std::vector<trade::CFunds>& accounts, const std::vector<trade::CRule>& rules, const std::vector<trade::CCalendar>& calendar);

	trade::CResult Send(trade::CReqParam request, _TyReplyHandler handler, std::uint64_t instance);
	trade::CAccountData Snapshot(const _TyAccount& account) const;

	void OnDepth(trade::CDepth depth) override;
	void OnMarketState(bool bAvailable) override;

	bool OnMarketResponse(const CRequest& request);
	void OnSessionState(SessionState state, const std::string& message);
	void SetDepthSubscriptionHandler(_TyDepthSubscriptionHandler&& handler);

private:
	CSimulatedDispatcher(CSimulatedDispatcher&&) = delete;
	CSimulatedDispatcher(const CSimulatedDispatcher& arg) = delete;
	CSimulatedDispatcher& operator=(const CSimulatedDispatcher& arg) = delete;
	CSimulatedDispatcher& operator=(CSimulatedDispatcher&&) = delete;

	void Run();
	void Refresh(const _TyAccount& account = {}, std::uint64_t instance = 0);
	void UpdateDepthDemand(const trade::COrder& order);
	void SyncDepthSubscriptions(bool stopping = false);

	struct CSessionSlot
	{
		std::uint64_t m_instance{ 0 };
		std::weak_ptr<class CSimulatedSession> m_session;
	};
	std::map<_TyAccount, CSessionSlot> m_sessions;
	std::uint64_t m_nextInstance{ 0 };
	std::mutex m_mtx_lifecycle;
	std::unique_ptr<CSimulatedCore> m_counter;
	_TyClock m_clock;
	mutable std::mutex m_mtx_state;
	std::condition_variable m_condition;
	std::deque<std::function<void()>> m_work;
	std::map<_TyAccount, trade::CAccountData> m_snapshots;
	std::uint64_t m_version{ 0 };
	bool m_bReady{ false };
	std::uint64_t m_delivered{ 0 };
	std::atomic_bool m_bInputAvailable{ false };
	std::atomic_uint64_t m_epoch{ 0 };
	std::uint64_t m_appliedEpoch{ 0 };
	bool m_bStopping{ true };
	// The worker owns order demand and subscription state; the handler uses m_mtx_state.
	_TyDepthSubscriptionHandler m_handler_depthSubscription;
	std::map<std::uint64_t, std::string> m_activeOrders;
	std::map<std::string, std::size_t> m_depthReferences;
	std::set<std::string> m_depthSubscriptions;
	std::uint64_t m_subscriptionEpoch{ 0 };
	std::thread m_worker;
};
