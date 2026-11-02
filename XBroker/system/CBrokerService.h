#ifndef XBROKER_SYSTEM_CBROKERSERVICE_H
#define XBROKER_SYSTEM_CBROKERSERVICE_H

#include "../service/CTradeService.h"
#include "../network/common_net.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

namespace net
{
	class CTcpServer;
}
class CSession;
class CSimulatedDispatcher;
class CZhongTaiDispatcher;

class CBrokerService final
{
public:
	CBrokerService(net::CTcpServer* pTcpServer, CSession* pSession);
	~CBrokerService();
	CBrokerService(const CBrokerService& arg) = delete;
	CBrokerService& operator=(const CBrokerService& arg) = delete;

	void Stop();
	bool Initialize();
	const std::string& GetLastError() const;

private:
	bool Broadcast(const CRequest& push);
	int OnNetEvent(const net::CNetEvent& event);

	trade::CResult InitializeCounters();

private:
	// Dispatchers outlive the trader and all borrowed callbacks; Stop controls teardown.
	std::unique_ptr<CSimulatedDispatcher> m_dispatcher_simulated;
	std::unique_ptr<CZhongTaiDispatcher> m_dispatcher_zhongtai;
	CTradeService m_trader;
	std::string m_strToken;
	std::string m_strLastError;
	std::mutex m_mtx_clients;
	std::unordered_set<net::_TyConnectionId> m_clients;
	bool m_bInitialized{ false };
	// BootLoader owns both endpoints and outlives this service.
	net::CTcpServer* m_pTcpServer{ nullptr };
	CSession* m_pSession{ nullptr };
};

#endif
