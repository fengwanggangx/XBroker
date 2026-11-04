#pragma once
#include "ITradeSession.h"
#include "CStateEdit.h"
#include <memory>

// Single-writer deterministic core. The host supplies serialization, time and market input.
class CSimulatedCore final
{
public:
	explicit CSimulatedCore(std::unique_ptr<trade::ICounterStore> store);
	trade::CResult Start(std::int64_t nNow);
	void Stop();
	trade::CResult Renew();
	trade::CResult AddAccount(const _TyAccount& account, std::int64_t nInitialBalance);
	trade::CResult ImportRules(const std::vector<trade::CRule>& rules, const std::vector<trade::CCalendar>& calendar);
	trade::CCounterReply Execute(const trade::CReqParam& request, std::int64_t nNow);
	trade::CResult OnDepth(trade::CDepth depth, std::int64_t nNow);
	void OnMarketState(bool bAvailable);
	void ForgetDepth(const std::string& security);
	trade::CResult Advance(std::int64_t nNow);
	trade::CAccountData Snapshot(const _TyAccount& account, std::int64_t nNow) const;
	std::vector<trade::CCountEvent> EventsAfter(std::uint64_t nSequence) const;
	std::vector<trade::CFunds> Accounts() const;
	bool Ready() const;
	std::uint64_t Version() const;

private:
	void CompleteReply(trade::CCounterReply& reply, std::uint64_t nOrderId) const;
	trade::CResult Commit(trade::CStateEdit& edit);
	trade::CResult Validate(const trade::CState& state) const;
	trade::CResult Submit(trade::CStateEdit& edit, trade::COrder order, std::int64_t nNow, std::uint64_t& nOrderId);
	void Cancel(trade::CStateEdit& edit, trade::COrder& order, trade::Status status, std::uint64_t nRequestId);
	trade::CResult Match(trade::CStateEdit& edit, trade::CDepth& depth, std::int64_t nNow);
	trade::CResult Fill(trade::CStateEdit& edit, trade::COrder& order, const trade::CRule& rule, std::int64_t nPrice, std::int64_t nQuantity, std::int64_t nNow);
	void Emit(trade::CState& state, trade::EventTy kind, const trade::COrder& order, const trade::CTrade& trade = {}, std::uint64_t nRequestId = 0);
	const trade::CCalendar* Calendar(std::int64_t nNow) const;
	std::unique_ptr<trade::ICounterStore> m_store;
	trade::CState m_state;
	std::int64_t m_nStartTime{ 0 };
	std::map<std::string, trade::CDepth> m_depths;
	bool m_bReady{ false }, m_bMarketAvailable{ false };
};
