#pragma once
#include "../network/common_net.h"
#include "../request/request.h"
#include "../counter/TradeTypes.h"
#include <functional>
#include <mutex>
#include <string>
#include <unordered_set>

class CTradeService;
enum class SessionState;

class CBrokerCenter final
{
public:
	explicit CBrokerCenter(CTradeService* pTradeService);
	~CBrokerCenter();
	CBrokerCenter(const CBrokerCenter& arg) = delete;
	CBrokerCenter& operator=(const CBrokerCenter& arg) = delete;

	void Stop();
	bool Initialize();
	const std::string& GetLastError() const;

	bool OnHQMarketReply(const CRequest& request);
	int OnClientNetEvent(const net::CNetEvent& event);
	void OnHQMarketState(SessionState state, const std::string& message);

	void SetMarketHandler(_TyRequestNotify&& handler);

private:
	void OnCounterEvent(const trade::CCountEvent& ev);
	void OnCounterReply(const trade::CCounterReply& reply);

	bool Broadcast(const CRequest& req);
	bool OnReply(net::_TyConnectionId id, const CRequest& req);
	bool ChangeDepthSubscription(const std::string& security, bool subscribe);

	CTradeService* m_pTradeService{ nullptr };
	_TyRequestNotify m_handler_market;
	std::string m_strToken;
	std::string m_strLastError;
	std::mutex m_mtx_clients;
	std::unordered_set<net::_TyConnectionId> m_clients;
	bool m_bInitialized{ false };
};
