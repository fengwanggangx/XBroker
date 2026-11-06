#pragma once
#include "AccountInfo.h"
#include "CCounterFactory.h"
#include "../request/request.h"
#include <functional>
#include <mutex>
#include <memory>
#include <vector>
#include <unordered_map>
#include <map>
#include <chrono>
#include <condition_variable>
#include <thread>

class CRequest;
class CSimulatedDispatcher;
class CZhongTaiDispatcher;
enum class SessionState;

struct CPendingCounterRequest
{
	net::_TyConnectionId m_connection{ -1 };
	std::uint64_t m_requestId{ 0 };
	std::uint64_t m_sourceId{ 0 };
	std::uint64_t m_correlationId{ 0 };
	_TyRequest m_type{ _TyRequest::UNKNOWN };
	trade::Operation m_operation{ trade::Operation::Funds };
	_TyAccount m_account;
	std::shared_ptr<trade::ITradeSession> m_session;
	std::string m_brokerName;
	std::string m_site;
	std::chrono::steady_clock::time_point m_deadline;
};

class CTradeService final
{
public:
	using _TyDepthHandler = std::function<bool(const std::string&, bool)>;
	using _TyCounterEventHandler = trade::ITradeSession::_TyEventHandler;
	using _TyReplyHandler = std::function<bool(net::_TyConnectionId, const CRequest&)>;
	using _TyCounterReplyHandler = trade::ITradeSession::_TyReplyHandler;

	CTradeService();
	~CTradeService();
	CTradeService(const CTradeService& arg) = delete;
	CTradeService& operator=(const CTradeService& arg) = delete;

	void Stop();
	bool Initialize();

	trade::CCounterFactory& CounterFactory();
	const std::string& GetLastError() const;
	bool HasAccount(const _TyAccount& account) const;

	bool OnRequest(const CRequest& req);
	bool OnHQMarketReply(const CRequest& req);
	void OnCounterReply(const trade::CCounterReply& reply);
	void OnCounterEvent(const trade::CCountEvent& ev);
	void OnHQMarketState(SessionState state, const std::string& message);

	void SetReplyHandler(_TyReplyHandler&& handler);
	// Bind callbacks before initialization; detach only after Stop drains the counters.
	void SetDepthHandler(_TyDepthHandler&& handler);
	void SetPushHandler(_TyRequestNotify&& handler);
	void SetCounterReplyHandler(_TyCounterReplyHandler&& handler);
	void SetCounterEventHandler(_TyCounterEventHandler&& handler);

private:
	friend class CLoginServiceTests;

	bool HandleDealQuery(const CRequest& req);
	bool HandleFundsQuery(const CRequest& req);
	bool HandleOrderQuery(const CRequest& req);
	bool HandleOrderSubmit(const CRequest& req);
	bool HandleOrderCancel(const CRequest& req);
	bool HandleAccountLogin(const CRequest& req);
	bool HandlePositionQuery(const CRequest& req);
	bool HandleAccountListQuery(const CRequest& req);
	bool HandleAccountDisconnect(const CRequest& req);
	bool HandleQueryBrokerSupported(const CRequest& req);

	bool SendResponse(net::_TyConnectionId id, const CRequest& req) const;
	void SendError(net::_TyConnectionId id, const CRequest& req, int code, const std::string& message) const;
	bool LoadBrokers();
	trade::CResult InitializeCounters();

	void PendingLoop(std::stop_token token);
	void ExpirePendingRequests();
	void FailPendingRequests();
	void FinishPending(const CPendingCounterRequest& pending, const trade::CCounterReply& reply);
	void FinishLogin(const CPendingCounterRequest& pending, const trade::CCounterReply& reply);
	bool SendCounterRequest(const CRequest& req, const std::shared_ptr<trade::ITradeSession>& counter, trade::CReqParam&& param);
	bool SendRequest(const CRequest& req, trade::CReqParam&& param);
	std::shared_ptr<trade::ITradeSession> FindCounter(const CRequest& req, _TyAccount& account) const;

private:
	std::unordered_map<_TyRequest, _TyRequestNotify> m_handler_request;
	mutable std::mutex m_mtx_accounts;
	std::map<std::uint64_t, CBrokerInfo> m_brokers;
	std::map<_TyAccount, CTradeAccount> m_accounts;
	std::map<_TyAccount, std::shared_ptr<trade::ITradeSession>> m_logging;
	std::vector<std::shared_ptr<trade::ITradeSession>> m_retired;
	bool m_bStopping{ false };
	std::string m_strLastError;
	_TyRequestNotify m_handler_push;
	trade::CCounterFactory m_counterFactory;
	std::unique_ptr<CSimulatedDispatcher> m_dispatcher_simulated;
	std::unique_ptr<CZhongTaiDispatcher> m_dispatcher_zhongtai;
	_TyCounterReplyHandler m_counter_reply;
	_TyCounterEventHandler m_counter_event;
	_TyReplyHandler m_handler_reply;
	_TyDepthHandler m_handler_depth;
	std::mutex m_mtx_pending;
	std::condition_variable_any m_condition_pending;
	std::unordered_map<std::uint64_t, CPendingCounterRequest> m_pending;
	std::uint64_t m_nextCorrelationId{ 0 };
	bool m_bPendingStopping{ false };
	std::jthread m_pendingThread;
};
