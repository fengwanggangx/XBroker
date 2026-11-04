#pragma once
#include "../ITradeSession.h"
#include <chrono>
#include <functional>
#include <memory>
namespace trade::zhongtai
{
	struct CSite
	{
		std::uint64_t m_brokerId{ 0 };
		std::string m_name, m_host, m_localIp;
		int m_port{ 0 };
	};
	struct CConfig
	{
		std::vector<CSite> m_sites;
		std::string m_softwareKey, m_softwareVersion, m_logPath;
		unsigned int m_clientId{ 1 };
		std::size_t m_queueLimit{ 4096 };
		std::int64_t m_queryTimeoutMs{ 10000 }, m_queryIntervalMs{ 200 }, m_refreshMs{ 5000 };
	};
	enum class ApiEventType
	{
		Disconnected,
		Error,
		Order,
		Trade,
		Position,
		Funds,
		CancelError,
		UnknownOrder
	};
	struct CApiEvent
	{
		ApiEventType m_type{ ApiEventType::Error };
		std::uint64_t m_session{ 0 }, m_cancelOrder{ 0 };
		int m_requestId{ 0 }, m_errorCode{ 0 };
		bool m_last{ false }, m_hasRow{ false };
		std::int64_t m_reference{ 0 };
		CResult m_result;
		COrder m_order;
		CTrade m_trade;
		CPosition m_position;
		CFunds m_funds;
	};
	struct CLoginReply
	{
		CResult m_result;
		std::uint64_t m_session{ 0 };
		int m_day{ 0 };
		bool m_authFailure{ false };
	};
	class IZhongTaiApi
	{
	public:
		using _TyHandler = std::function<void(CApiEvent)>;
		virtual ~IZhongTaiApi() = default;
		virtual void Stop() = 0;
		virtual void SetHandler(_TyHandler&& handler) = 0;
		virtual CLoginReply Login(const CSite& site, const std::string& account, const std::string& password) = 0;
		virtual void Logout(std::uint64_t session) = 0;
		virtual CResult Query(std::uint64_t session, Operation operation, int requestId, std::int64_t reference) = 0;
		virtual CResult Submit(std::uint64_t session, const COrder& order, std::uint64_t& brokerId) = 0;
		virtual CResult Cancel(std::uint64_t session, std::uint64_t brokerId) = 0;
	};
	std::unique_ptr<IZhongTaiApi> CreateZhongTaiApi(const CConfig& config);
	std::int64_t Now();
} // namespace trade::zhongtai
