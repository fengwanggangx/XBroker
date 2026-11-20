#pragma once
#include "../request/AccountIdentity.h"
#include <memory>
#include <set>

namespace trade
{
	class ITradeSession;
}

enum class TradeAccountState
{
	Disconnected,
	Connected,
	Error,
	Connecting,
	Synchronizing,
	Reconnecting
};

struct CBrokerInfo
{
	std::uint64_t m_broker_id{ 0 };
	std::string m_name;
	std::string m_adapter;
	std::set<std::string> m_sites;
};

struct CTradeAccountInfo
{
	std::uint64_t m_broker_id{ 0 };
	std::string m_site;
	std::string m_account;
	std::string m_broker_name;
	std::string m_name;
	TradeAccountState m_state{ TradeAccountState::Disconnected };
	std::string m_last_error;
};

inline _TyAccount AccountIndex(const CTradeAccountInfo& info)
{
	return { info.m_broker_id, info.m_account };
}

inline CTradeAccountInfo AccountInfo(const _TyAccount& index)
{
	CTradeAccountInfo info;
	info.m_broker_id = index.first;
	info.m_account = index.second;
	return info;
}

struct CTradeAccount
{
	CTradeAccountInfo m_info;
	// Pending operations may retain the account session until their response completes.
	std::shared_ptr<trade::ITradeSession> m_session;
};
