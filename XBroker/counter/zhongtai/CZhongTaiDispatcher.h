#pragma once
#include "CZhongTaiStore.h"
#include "../CounterConfig.h"
#include "CZhongTaiSession.h"
#include <condition_variable>
#include <deque>
#include <thread>
#include <optional>
#include <atomic>
#include <set>
class CZhongTaiDispatcher final
{
public:
	CZhongTaiDispatcher() = default;
	CZhongTaiDispatcher(trade::zhongtai::CConfig config, std::unique_ptr<trade::zhongtai::IZhongTaiApi> api, std::unique_ptr<trade::zhongtai::IZhongTaiStore> store);
	~CZhongTaiDispatcher();
	void Stop();
	void Start();
	bool IsInitialized() const;
	trade::CResult Initialize();
	void Receive(trade::zhongtai::CApiEvent event);
	bool IsCurrent(const _TyAccount& account, std::uint64_t instance);
	std::shared_ptr<trade::ITradeSession> Create(const trade::CCounterConfig& config);
	trade::CResult Send(trade::CReqParam request, trade::ITradeSession::_TyReplyHandler handler, std::uint64_t instance);

private:
	struct CWork
	{
		trade::CReqParam m_request;
		std::uint64_t m_instance{ 0 };
		trade::ITradeSession::_TyReplyHandler m_handler;
	};
	struct CSession
	{
		_TyAccount m_account;
		std::weak_ptr<trade::zhongtai::CZhongTaiSession> m_counter;
		std::uint64_t m_session{ 0 }, m_generation{ 0 }, m_instance{ 0 };
		std::string m_password, m_site;
		request::ZhongTaiJournal m_journal;
		trade::CAccountData m_snapshot;
		std::map<std::uint64_t, trade::COrder> m_orders;
		std::map<std::string, trade::CTrade> m_trades;
		std::deque<CWork> m_queries;
		std::vector<trade::zhongtai::CApiEvent> m_buffer;
		std::vector<trade::CCountEvent> m_initial_events;
		std::optional<CWork> m_login;
		int m_sync_step{ 0 }, m_attempt{ 0 };
		bool m_ready{ false }, m_published{ false }, m_refresh{ false }, m_disabled{ false }, m_dirty_financials{ false };
		std::int64_t m_retry_at{ 0 }, m_refresh_at{ 0 }, m_lease_at{ 0 };
	};
	struct CQuery
	{
		_TyAccount m_account;
		std::uint64_t m_session{ 0 }, m_generation{ 0 };
		trade::Operation m_operation{ trade::Operation::Funds };
		int m_id{ 0 };
		bool m_sync{ false }, m_page_pending{ false };
		std::int64_t m_deadline{ 0 }, m_reference{ 0 };
		std::size_t m_rows{ 0 };
		std::vector<trade::zhongtai::CApiEvent> m_data;
		std::optional<CWork> m_work;
	};
	void Run();
	void Tick();
	void FinishQuery();
	void Handle(CWork work);
	void Login(CSession& session);
	bool Persist(CSession& session);
	void Snapshot(CSession& session);
	void Handle(trade::zhongtai::CApiEvent event);
	void Publish(CSession& session, trade::CCountEvent event);
	void Apply(CSession& session, const trade::zhongtai::CApiEvent& event);
	void Fail(CSession& session, const trade::CResult& result, bool disable = false);
	void Reply(CSession& session, CWork& work, const trade::CResult& result, std::uint64_t orderId = 0);
	void Query(CSession& session, trade::Operation operation, bool sync, std::optional<CWork> work = {});
	trade::zhongtai::CConfig m_config;
	std::unique_ptr<trade::zhongtai::IZhongTaiApi> m_api;
	std::unique_ptr<trade::zhongtai::IZhongTaiStore> m_store;
	std::map<_TyAccount, CSession> m_sessions;
	std::optional<CQuery> m_query;
	int m_next_query{ 1 };
	std::set<std::uint64_t> m_used_sessions;
	std::int64_t m_next_query_at{ 0 };
	_TyAccount m_last_scheduled;
	std::mutex m_queue_mutex;
	std::mutex m_stop_mutex;
	std::condition_variable m_condition;
	std::deque<std::variant<CWork, trade::zhongtai::CApiEvent>> m_queue;
	std::atomic_bool m_overflow{ false };
	bool m_stopping{ false };
	std::thread m_worker;
	std::thread::id m_worker_id;
	std::mutex m_counters_mutex;
	std::map<_TyAccount, std::uint64_t> m_instances;
	std::map<_TyAccount, std::weak_ptr<trade::zhongtai::CZhongTaiSession>> m_counters;
	std::uint64_t m_next_instance{ 0 };
};
