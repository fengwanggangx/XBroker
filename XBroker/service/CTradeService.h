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

class CRequest;

class CTradeService final
{
public:

	CTradeService();
	~CTradeService();
	CTradeService(const CTradeService& arg) = delete;
	CTradeService& operator=(const CTradeService& arg) = delete;

	void Stop();
	bool Initialize();

	trade::CCounterFactory& CounterFactory();
	const std::string& GetLastError() const;
	bool HasAccount(const _TyAccount& account) const;

	bool HandleTradeRequest(const CRequest& req);
	bool HandleAccountRequest(const CRequest& req);

	void SetPushHandler(_TyRequestNotify&& handler);

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

	bool LoadBrokers();
	bool DispatchRequest(const CRequest& req);
	void OnEvent(const trade::CCountEvent& event);
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
};
