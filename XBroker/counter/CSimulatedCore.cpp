#include "CSimulatedCore.h"
#include <algorithm>
#include <limits>
#include <set>

namespace
{
	constexpr std::int64_t MaxAgeMs = 5000;
	trade::CResult Reject(const std::string& strMessage)
	{
		return { trade::Error::Rejected, strMessage };
	}
	bool Add(std::int64_t nLeft, std::int64_t nRight, std::int64_t& nValue)
	{
		if ((0 > nLeft) || (0 > nRight) || (std::numeric_limits<std::int64_t>::max() - nLeft < nRight))
		{
			return false;
		}
		nValue = nLeft + nRight;
		return true;
	}
	bool Money(std::int64_t nPrice, std::int64_t nQuantity, int nScale, std::int64_t& nValue)
	{
		if ((0 > nPrice) || (0 > nQuantity) || (0 > nScale) || (6 < nScale) || ((0 != nQuantity) && (std::numeric_limits<std::int64_t>::max() / nQuantity < nPrice)))
		{
			return false;
		}
		std::int64_t nProduct = nPrice * nQuantity;
		std::int64_t nDivisor = 1;
		for (int nIndex = 0; nIndex < nScale; ++nIndex)
		{
			nDivisor *= 10;
		}
		// Reject sub-cent turnover: rounding every partial fill would otherwise change the bill.
		if ((2 <= nScale) && (0 != nProduct % (nDivisor / 100)))
		{
			return false;
		}
		if (2 <= nScale)
		{
			nValue = nProduct / (nDivisor / 100);
			return true;
		}
		std::int64_t nMultiplier = 100 / nDivisor;
		if (std::numeric_limits<std::int64_t>::max() / nMultiplier < nProduct)
		{
			return false;
		}
		nValue = nProduct * nMultiplier;
		return true;
	}
	bool Rate(std::int64_t nMoney, std::int64_t nPpm, std::int64_t& nFee)
	{
		if ((0 > nPpm) || (1000000 < nPpm))
		{
			return false;
		}
		std::int64_t nRemainder = nMoney % 1000000 * nPpm;
		return Add(nMoney / 1000000 * nPpm, nRemainder / 1000000 + (0 != nRemainder % 1000000 ? 1 : 0), nFee);
	}
	bool Fee(std::int64_t nTurnover, const trade::CRule& rule, trade::Side side, std::int64_t& nFee)
	{
		if (0 == nTurnover)
		{
			nFee = 0;
			return true;
		}
		std::int64_t nCommission = 0, nTransfer = 0, nTax = 0;
		if (!Rate(nTurnover, rule.m_nCommissionPpm, nCommission) || !Rate(nTurnover, rule.m_nTransferPpm, nTransfer) || !Rate(nTurnover, trade::Side::Sell == side ? rule.m_nSellTaxPpm : 0, nTax))
		{
			return false;
		}
		return Add(std::max(nCommission, rule.m_nMinimumFee), nTransfer, nFee) && Add(nFee, nTax, nFee);
	}
	bool InSession(const trade::CCalendar& day, std::int64_t nNow)
	{
		return ((day.m_nOpen <= nNow) && (nNow < day.m_nBreak)) || ((day.m_nResume <= nNow) && (nNow < day.m_nClose));
	}
} // namespace
CSimulatedCore::CSimulatedCore(std::unique_ptr<trade::ICounterStore> store) : m_store(std::move(store))
{
}
trade::CResult CSimulatedCore::Start(std::int64_t nNow)
{
	trade::CResult result = m_store->Acquire(m_state);
	if (!result.Ok())
	{
		return result;
	}
	result = Validate(m_state);
	m_bReady = result.Ok();
	if (!m_bReady)
	{
		m_store->Release();
		return result;
	}
	m_depths.clear();
	m_nStartTime = nNow;
	m_bMarketAvailable = false;
	// A restored session is not a live transport connection.
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	for (auto& item : next.m_accounts)
	{
		edit.Save(&trade::CState::m_accounts, item.first);
		item.second.m_bConnected = false;
	}
	result = Commit(edit);
	return result.Ok() ? Advance(nNow) : result;
}
void CSimulatedCore::Stop()
{
	m_bReady = false;
	m_bMarketAvailable = false;
	m_depths.clear();
	m_store->Release();
}
trade::CResult CSimulatedCore::Renew()
{
	if (!m_bReady)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	trade::CResult result = m_store->Renew();
	if (!result.Ok())
	{
		m_bReady = false;
		m_depths.clear();
	}
	return result;
}
std::uint64_t CSimulatedCore::Version() const
{
	return m_state.m_sequence;
}
bool CSimulatedCore::Ready() const
{
	return m_bReady;
}
trade::CResult CSimulatedCore::Validate(const trade::CState& state) const
{
	std::map<_TyAccount, std::int64_t> frozen;
	std::map<std::pair<_TyAccount, std::string>, std::int64_t> shares;
	std::set<std::pair<_TyAccount, std::uint64_t>> clients;
	std::map<std::uint64_t, std::int64_t> filled, turnover, fees;
	for (const auto& item : state.m_trades)
	{
		const trade::CTrade& trade = item.second;
		std::int64_t nMoney = 0;
		auto order = state.m_orders.find(trade.m_orderId);
		if (item.first != trade.m_tradeId || state.m_nextTrade <= item.first || state.m_orders.end() == order || order->second.m_accountIndex != trade.m_accountIndex || order->second.m_strSecurity != trade.m_strSecurity || order->second.m_side != trade.m_side || 0 >= trade.m_nQuantity || 0 > trade.m_nFee || !Money(trade.m_nPrice, trade.m_nQuantity, trade.m_nPriceScale, nMoney) || !Add(filled[trade.m_orderId], trade.m_nQuantity, filled[trade.m_orderId]) || !Add(turnover[trade.m_orderId], nMoney, turnover[trade.m_orderId]) || !Add(fees[trade.m_orderId], trade.m_nFee, fees[trade.m_orderId]))
		{
			return Reject("trade journal invariant failed");
		}
	}

	for (const auto& item : state.m_rules)
	{
		const trade::CRule& rule = item.second;
		if (!state.m_calendar.contains(rule.m_nDay) || 0 >= rule.m_nTick || 0 >= rule.m_nBuyStep || 0 >= rule.m_nSellStep || 0 >= rule.m_nBuyMinimum || 0 >= rule.m_nSellMinimum || 0 >= rule.m_nLower || rule.m_nUpper < rule.m_nLower || 0 > rule.m_nPriceScale || 6 < rule.m_nPriceScale || 0 > rule.m_nCommissionPpm || 1000000 < rule.m_nCommissionPpm || 0 > rule.m_nMinimumFee || 0 > rule.m_nSellTaxPpm || 1000000 < rule.m_nSellTaxPpm || 0 > rule.m_nTransferPpm || 1000000 < rule.m_nTransferPpm)
		{
			return Reject("persisted rule invalid");
		}
	}
	std::uint64_t nLastSequence = 0;
	for (const auto& event : state.m_events)
	{
		if ((nLastSequence + 1 != event.m_sequence) || (event.m_sequence > state.m_sequence))
		{
			return Reject("event sequence corrupted");
		}
		nLastSequence = event.m_sequence;
	}
	if (nLastSequence != state.m_sequence)
	{
		return Reject("event tail missing");
	}
	for (const auto& item : state.m_orders)
	{
		const trade::COrder& order = item.second;
		if (0 == order.m_clientId || 0 >= order.m_nQuantity || 0 >= order.m_nPrice || trade::Status::Accepted > order.m_status || trade::Status::Rejected < order.m_status || (trade::Side::Buy != order.m_side && trade::Side::Sell != order.m_side) || filled[item.first] != order.m_nFilled || turnover[item.first] != order.m_nTurnover || fees[item.first] != order.m_nFee)
		{
			return Reject("order/trade journal mismatch");
		}

		if ((item.first != order.m_orderId) || (state.m_nextOrder <= order.m_orderId) || !state.m_accounts.contains(order.m_accountIndex) || !clients.emplace(order.m_accountIndex, order.m_clientId).second || (0 > order.m_nFilled) || (order.m_nFilled > order.m_nQuantity) || (0 > order.m_nFrozen))
		{
			return Reject("invalid persisted order");
		}
		if (trade::IsActive(order.m_status))
		{
			if (trade::Side::Buy == order.m_side)
			{
				if (!Add(frozen[order.m_accountIndex], order.m_nFrozen, frozen[order.m_accountIndex]))
				{
					return Reject("frozen overflow");
				}
			}
			else
			{
				if (!Add(shares[{ order.m_accountIndex, order.m_strSecurity }], order.m_nQuantity - order.m_nFilled, shares[{ order.m_accountIndex, order.m_strSecurity }]))
				{
					return Reject("share overflow");
				}
			}
		}
		else if (0 != order.m_nFrozen)
		{
			return Reject("terminal order still frozen");
		}
	}
	for (const auto& item : state.m_accounts)
	{
		if ((item.first != item.second.m_accountIndex) || (0 > item.second.m_nBalance) || (0 > item.second.m_nFrozen) || (item.second.m_nBalance < item.second.m_nFrozen) || (frozen[item.first] != item.second.m_nFrozen))
		{
			return Reject("cash ledger invariant failed");
		}
	}
	for (const auto& item : state.m_positions)
	{
		const trade::CPosition& position = item.second;
		std::int64_t nTotal = 0;
		if (!state.m_accounts.contains(position.m_accountIndex) || !Add(position.m_nAvailable, position.m_nFrozen, nTotal) || !Add(nTotal, position.m_nToday, nTotal) || (nTotal != position.m_nTotal) || (position.m_nFrozen != shares[item.first]) || (0 > position.m_nCost))
		{
			return Reject("position ledger invariant failed");
		}
	}
	for (const auto& item : shares)
	{
		if ((0 != item.second) && !state.m_positions.contains(item.first))
		{
			return Reject("missing frozen position");
		}
	}
	return {};
}
trade::CResult CSimulatedCore::Commit(trade::CStateEdit& edit)
{
	trade::CResult result = Validate(m_state);
	if (result.Ok())
	{
		result = m_store->Commit(edit.Changes(), edit.Sequence());
		if (!result.Ok())
		{
			m_bReady = false;
			m_depths.clear();
		}
	}
	if (result.Ok())
	{
		edit.Accept();
	}
	else
	{
		edit.Rollback();
	}
	return result;
}
trade::CResult CSimulatedCore::AddAccount(const _TyAccount& account, std::int64_t nInitialBalance)
{
	if (!m_bReady)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	if (m_state.m_accounts.contains(account))
	{
		return { trade::Error::Conflict, "account already exists" };
	}
	if ((!IsAccountValid(account)) || (0 >= nInitialBalance))
	{
		return Reject("invalid account opening parameters");
	}
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	edit.Save(&trade::CState::m_accounts, account);
	next.m_accounts.emplace(account, trade::CFunds{ account, nInitialBalance, 0, false, true });
	trade::COrder order;
	order.m_accountIndex = account;
	Emit(next, trade::EventTy::Funds, order);
	return Commit(edit);
}
trade::CResult CSimulatedCore::ImportRules(const std::vector<trade::CRule>& rules, const std::vector<trade::CCalendar>& calendar)
{
	if (!m_bReady)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	for (const auto& day : calendar)
	{
		if ((0 >= day.m_nDay) || !(day.m_nOpen < day.m_nBreak && day.m_nBreak < day.m_nResume && day.m_nResume < day.m_nClose))
		{
			return Reject("invalid calendar");
		}
		auto existing = next.m_calendar.find(day.m_nDay);
		// Existing calendar entries cannot be changed after orders have been accepted.
		if ((next.m_calendar.end() != existing) && !next.m_orders.empty() && ((existing->second.m_nOpen != day.m_nOpen) || (existing->second.m_nBreak != day.m_nBreak) || (existing->second.m_nResume != day.m_nResume) || (existing->second.m_nClose != day.m_nClose)))
		{
			return Reject("calendar immutable after trading");
		}
		edit.Save(&trade::CState::m_calendar, day.m_nDay);
		next.m_calendar[day.m_nDay] = day;
	}
	std::int64_t nPreviousClose = 0;
	for (const auto& entry : next.m_calendar)
	{
		if (entry.second.m_nOpen <= nPreviousClose)
		{
			return Reject("overlapping or unordered calendar");
		}
		nPreviousClose = entry.second.m_nClose;
	}
	for (const auto& rule : rules)
	{
		if (rule.m_strVersion.empty() || rule.m_strSecurity.empty() || !next.m_calendar.contains(rule.m_nDay) || (0 > rule.m_nPriceScale) || (6 < rule.m_nPriceScale) || (0 >= rule.m_nTick) || (0 >= rule.m_nBuyMinimum) || (0 >= rule.m_nBuyStep) || (0 >= rule.m_nSellMinimum) || (0 >= rule.m_nSellStep) || (0 >= rule.m_nLower) || (rule.m_nLower > rule.m_nUpper) || (0 > rule.m_nMinimumFee) || (0 > rule.m_nCommissionPpm) || (1000000 < rule.m_nCommissionPpm) || (0 > rule.m_nSellTaxPpm) || (1000000 < rule.m_nSellTaxPpm) || (0 > rule.m_nTransferPpm) || (1000000 < rule.m_nTransferPpm))
		{
			return Reject("invalid security rule");
		}
		bool bExchange = rule.m_strSecurity.ends_with(".SSE") || rule.m_strSecurity.ends_with(".SZSE");
		if (!bExchange || (("MAIN" != rule.m_strBoard) && ("CHINEXT" != rule.m_strBoard) && ("STAR" != rule.m_strBoard)))
		{
			return { trade::Error::Unsupported, "unsupported security board" };
		}
		std::pair<int, std::string> key = std::make_pair(rule.m_nDay, rule.m_strSecurity);
		auto previous = next.m_rules.find(key);
		if (next.m_rules.end() != previous)
		{
			trade::CState left, right;
			left.m_rules.emplace(key, previous->second);
			right.m_rules.emplace(key, rule);
			if (trade::EncodeState(left) != trade::EncodeState(right))
			{
				return Reject("rule immutable; import a new trading day/version");
			}
		}
		edit.Save(&trade::CState::m_rules, key);
		next.m_rules[key] = rule;
	}
	return Commit(edit);
}
const trade::CCalendar* CSimulatedCore::Calendar(std::int64_t nNow) const
{
	for (const auto& item : m_state.m_calendar)
	{
		if ((item.second.m_nOpen <= nNow) && (nNow < item.second.m_nClose))
		{
			return &item.second;
		}
	}
	return nullptr;
}
void CSimulatedCore::Emit(trade::CState& state, trade::EventTy kind, const trade::COrder& order, const trade::CTrade& trade, std::uint64_t nRequestId)
{
	trade::CCountEvent event;
	event.m_sequence = ++state.m_sequence;
	event.m_requestId = 0 == nRequestId ? order.m_requestId : nRequestId;
	event.m_accountIndex = order.m_accountIndex;
	event.m_kind = kind;
	event.m_order = order;
	event.m_trade = trade;
	event.m_funds = state.m_accounts.at(order.m_accountIndex);
	auto position = state.m_positions.find({ order.m_accountIndex, order.m_strSecurity });
	if (state.m_positions.end() != position)
	{
		event.m_position = position->second;
	}
	state.m_events.emplace_back(std::move(event));
}
trade::CResult CSimulatedCore::Submit(trade::CStateEdit& edit, trade::COrder order, std::int64_t nNow, std::uint64_t& nOrderId)
{
	trade::CState& state = edit.State();
	for (const auto& item : state.m_orders)
	{
		const trade::COrder& previous = item.second;
		if ((previous.m_accountIndex == order.m_accountIndex) && ((previous.m_clientId == order.m_clientId) || ((0 != order.m_strategyId) && (previous.m_strategyId == order.m_strategyId) && (previous.m_signalId == order.m_signalId))))
		{
			if ((previous.m_strSecurity != order.m_strSecurity) || (previous.m_side != order.m_side) || (previous.m_nPrice != order.m_nPrice) || (previous.m_nQuantity != order.m_nQuantity) || (previous.m_nPriceScale != order.m_nPriceScale) || (previous.m_strategyId != order.m_strategyId) || (previous.m_signalId != order.m_signalId))
			{
				return { trade::Error::Conflict, "client order ID reused with different parameters" };
			}
			nOrderId = previous.m_orderId;
			return {};
		}
	}
	const trade::CCalendar* day = Calendar(nNow);
	if ((nullptr == day) || !InSession(*day, nNow))
	{
		return { trade::Error::Unsupported, "only continuous trading sessions supported" };
	}
	auto ruleIter = state.m_rules.find({ day->m_nDay, order.m_strSecurity });
	if (state.m_rules.end() == ruleIter)
	{
		return Reject("missing security rules");
	}
	const trade::CRule& rule = ruleIter->second;
	if (rule.m_bCorporateAction)
	{
		return Reject("corporate action pending");
	}
	if ((0 == order.m_clientId) || (0 >= order.m_nQuantity) || (rule.m_nPriceScale != order.m_nPriceScale) || (rule.m_nLower > order.m_nPrice) || (rule.m_nUpper < order.m_nPrice) || (0 != order.m_nPrice % rule.m_nTick))
	{
		return Reject("invalid limit order");
	}
	order.m_orderId = 0;
	order.m_nFilled = 0;
	order.m_nFrozen = 0;
	order.m_nTurnover = 0;
	order.m_nFee = 0;
	order.m_status = trade::Status::Accepted;
	if ((trade::Side::Buy != order.m_side) && (trade::Side::Sell != order.m_side))
	{
		return Reject("invalid side");
	}
	edit.Save(&trade::CState::m_accounts, order.m_accountIndex);
	trade::CFunds& account = state.m_accounts.at(order.m_accountIndex);
	if (!account.m_bConnected)
	{
		return { trade::Error::Unavailable, "account disconnected" };
	}
	if (!m_bMarketAvailable)
	{
		return { trade::Error::Unavailable, "market disconnected" };
	}
	std::int64_t nTurnover = 0, nFee = 0;
	if (!Money(order.m_nPrice, order.m_nQuantity, order.m_nPriceScale, nTurnover) || !Fee(nTurnover, rule, order.m_side, nFee))
	{
		return Reject("money overflow or sub-cent turnover");
	}
	if (trade::Side::Sell == order.m_side && nTurnover < nFee)
	{
		return Reject("sell proceeds do not cover order fees");
	}
	if (trade::Side::Buy == order.m_side)
	{
		if ((order.m_nQuantity < rule.m_nBuyMinimum) || (0 != (order.m_nQuantity - rule.m_nBuyMinimum) % rule.m_nBuyStep) || !Add(nTurnover, nFee, order.m_nFrozen) || (order.m_nFrozen > account.m_nBalance - account.m_nFrozen))
		{
			return Reject("invalid buy size or insufficient funds");
		}
		account.m_nFrozen += order.m_nFrozen;
	}
	else
	{
		auto position = state.m_positions.find({ order.m_accountIndex, order.m_strSecurity });
		if ((state.m_positions.end() == position) || (position->second.m_nAvailable < order.m_nQuantity) || (order.m_nQuantity < rule.m_nSellMinimum) || (0 != (order.m_nQuantity - rule.m_nSellMinimum) % rule.m_nSellStep))
		{
			return Reject("invalid sell size or insufficient available position");
		}
		edit.Save(&trade::CState::m_positions, position->first);
		position->second.m_nAvailable -= order.m_nQuantity;
		position->second.m_nFrozen += order.m_nQuantity;
	}
	order.m_orderId = state.m_nextOrder++;
	order.m_nDay = day->m_nDay;
	order.m_nTime = nNow;
	order.m_strRuleVersion = rule.m_strVersion;
	nOrderId = order.m_orderId;
	edit.Save(&trade::CState::m_orders, order.m_orderId);
	state.m_orders.emplace(order.m_orderId, order);
	Emit(state, trade::EventTy::Order, order);
	Emit(state, trade::EventTy::Funds, order);
	if (trade::Side::Sell == order.m_side)
	{
		Emit(state, trade::EventTy::Position, order);
	}
	return {};
}
void CSimulatedCore::Cancel(trade::CStateEdit& edit, trade::COrder& order, trade::Status status, std::uint64_t nRequestId)
{
	trade::CState& state = edit.State();
	if (!trade::IsActive(order.m_status))
	{
		return;
	}
	edit.Save(&trade::CState::m_orders, order.m_orderId);
	edit.Save(&trade::CState::m_accounts, order.m_accountIndex);
	if (trade::Side::Sell == order.m_side)
	{
		edit.Save(&trade::CState::m_positions, std::make_pair(order.m_accountIndex, order.m_strSecurity));
	}
	if (trade::Side::Buy == order.m_side)
	{
		state.m_accounts.at(order.m_accountIndex).m_nFrozen -= order.m_nFrozen;
	}
	else
	{
		trade::CPosition& position = state.m_positions.at({ order.m_accountIndex, order.m_strSecurity });
		position.m_nAvailable += order.m_nQuantity - order.m_nFilled;
		position.m_nFrozen -= order.m_nQuantity - order.m_nFilled;
	}
	order.m_nFrozen = 0;
	order.m_status = status;
	Emit(state, trade::EventTy::Order, order, {}, nRequestId);
	Emit(state, trade::EventTy::Funds, order, {}, nRequestId);
	if (trade::Side::Sell == order.m_side)
	{
		Emit(state, trade::EventTy::Position, order, {}, nRequestId);
	}
}
trade::CResult CSimulatedCore::Fill(trade::CStateEdit& edit, trade::COrder& order, const trade::CRule& rule, std::int64_t nPrice, std::int64_t nQuantity, std::int64_t nNow)
{
	trade::CState& state = edit.State();
	std::int64_t nMoney = 0, nTurnover = 0, nTotalFee = 0;
	if (!Money(nPrice, nQuantity, order.m_nPriceScale, nMoney) || !Add(order.m_nTurnover, nMoney, nTurnover) || !Fee(nTurnover, rule, order.m_side, nTotalFee))
	{
		return Reject("fill arithmetic overflow");
	}
	std::int64_t nFee = nTotalFee - order.m_nFee;
	edit.Save(&trade::CState::m_accounts, order.m_accountIndex);
	trade::CFunds& account = state.m_accounts.at(order.m_accountIndex);
	std::pair<_TyAccount, std::string> key = std::make_pair(order.m_accountIndex, order.m_strSecurity);
	edit.Save(&trade::CState::m_orders, order.m_orderId);
	edit.Save(&trade::CState::m_positions, key);
	trade::CPosition& position = state.m_positions[key];
	position.m_accountIndex = order.m_accountIndex;
	position.m_strSecurity = order.m_strSecurity;
	std::int64_t nRemaining = order.m_nQuantity - order.m_nFilled - nQuantity;
	if (trade::Side::Buy == order.m_side)
	{
		std::int64_t nDebit = 0, nRemainingMoney = 0, nWorstTurnover = 0, nWorstFee = 0, nReserve = 0, nRequired = 0;
		if (!Add(nMoney, nFee, nDebit) || !Money(order.m_nPrice, nRemaining, order.m_nPriceScale, nRemainingMoney) || !Add(nTurnover, nRemainingMoney, nWorstTurnover) || !Fee(nWorstTurnover, rule, order.m_side, nWorstFee) || !Add(nRemainingMoney, nWorstFee - nTotalFee, nReserve) || (account.m_nBalance < nDebit) || !Add(nDebit, nReserve, nRequired) || (order.m_nFrozen < nRequired))
		{
			return Reject("invalid fill reserve");
		}
		account.m_nBalance -= nDebit;
		account.m_nFrozen -= order.m_nFrozen - nReserve;
		order.m_nFrozen = nReserve;
		if (!Add(position.m_nTotal, nQuantity, position.m_nTotal) || !Add(position.m_nToday, nQuantity, position.m_nToday) || !Add(position.m_nCost, nDebit, position.m_nCost))
		{
			return Reject("position overflow");
		}
	}
	else
	{
		if ((position.m_nFrozen < nQuantity) || (nMoney < nFee) || !Add(account.m_nBalance, nMoney - nFee, account.m_nBalance))
		{
			return Reject("invalid sell settlement");
		}
		// Proportional book cost, using quotient/remainder to avoid multiplication overflow.
		std::int64_t nCost = position.m_nCost / position.m_nTotal * nQuantity;
		std::int64_t nRemainder = position.m_nCost % position.m_nTotal;
		if ((0 != nQuantity) && (std::numeric_limits<std::int64_t>::max() / nQuantity < nRemainder))
		{
			return Reject("cost arithmetic overflow");
		}
		nCost += nRemainder * nQuantity / position.m_nTotal;
		position.m_nCost -= nCost;
		position.m_nTotal -= nQuantity;
		position.m_nFrozen -= nQuantity;
	}
	order.m_nFilled += nQuantity;
	order.m_nTurnover = nTurnover;
	order.m_nFee = nTotalFee;
	order.m_status = 0 == nRemaining ? trade::Status::Filled : trade::Status::PartiallyFilled;
	trade::CTrade trade{ state.m_nextTrade++, order.m_orderId, order.m_accountIndex, order.m_strSecurity, order.m_side, nPrice, nQuantity, nFee, nNow, order.m_nPriceScale };
	edit.Save(&trade::CState::m_trades, trade.m_tradeId);
	state.m_trades.emplace(trade.m_tradeId, trade);
	Emit(state, trade::EventTy::Deal, order, trade);
	Emit(state, trade::EventTy::Order, order);
	Emit(state, trade::EventTy::Funds, order);
	Emit(state, trade::EventTy::Position, order);
	return {};
}
trade::CResult CSimulatedCore::Match(trade::CStateEdit& edit, trade::CDepth& depth, std::int64_t nNow)
{
	trade::CState& state = edit.State();
	const trade::CCalendar* day = Calendar(nNow);
	if (!m_bMarketAvailable || (nullptr == day) || !InSession(*day, nNow) || (depth.m_nTime > nNow) || (MaxAgeMs < nNow - depth.m_nTime))
	{
		return {};
	}
	auto ruleIter = state.m_rules.find({ day->m_nDay, depth.m_strSecurity });
	if (state.m_rules.end() == ruleIter)
	{
		return {};
	}
	const trade::CRule& rule = ruleIter->second;
	if (rule.m_bCorporateAction || (rule.m_nPriceScale != depth.m_nPriceScale))
	{
		return {};
	}
	for (const auto& level : depth.m_bids)
	{
		if (level.m_nPrice < rule.m_nLower || level.m_nPrice > rule.m_nUpper || 0 != level.m_nPrice % rule.m_nTick)
		{
			return Reject("bid outside configured price rules");
		}
	}
	for (const auto& level : depth.m_asks)
	{
		if (level.m_nPrice < rule.m_nLower || level.m_nPrice > rule.m_nUpper || 0 != level.m_nPrice % rule.m_nTick)
		{
			return Reject("ask outside configured price rules");
		}
	}
	std::vector<std::uint64_t> orders;
	for (const auto& item : state.m_orders)
	{
		if (trade::IsActive(item.second.m_status) && (item.second.m_nDay == day->m_nDay) && (item.second.m_strSecurity == depth.m_strSecurity) && state.m_accounts.at(item.second.m_accountIndex).m_bConnected)
		{
			orders.emplace_back(item.first);
		}
	}
	std::sort(orders.begin(), orders.end(), [&state](std::uint64_t nLeft, std::uint64_t nRight)
	{
		const trade::COrder& left = state.m_orders.at(nLeft);
		const trade::COrder& right = state.m_orders.at(nRight);
		if (left.m_side != right.m_side)
		{
			return left.m_side < right.m_side;
		}
		if (left.m_nPrice != right.m_nPrice)
		{
			return trade::Side::Buy == left.m_side ? left.m_nPrice > right.m_nPrice : left.m_nPrice < right.m_nPrice;
		}
		return nLeft < nRight;
	});
	for (std::uint64_t nId : orders)
	{
		trade::COrder& order = state.m_orders.at(nId);
		std::vector<trade::CLevel>& levels = trade::Side::Buy == order.m_side ? depth.m_asks : depth.m_bids;
		for (auto& level : levels)
		{
			if (!trade::IsActive(order.m_status))
			{
				break;
			}
			if ((0 >= level.m_nQuantity) || (trade::Side::Buy == order.m_side ? level.m_nPrice > order.m_nPrice : level.m_nPrice < order.m_nPrice))
			{
				continue;
			}
			std::int64_t nQuantity = std::min(level.m_nQuantity, order.m_nQuantity - order.m_nFilled);
			trade::CResult result = Fill(edit, order, rule, level.m_nPrice, nQuantity, nNow);
			if (!result.Ok())
			{
				return result;
			}
			level.m_nQuantity -= nQuantity;
		}
	}
	return {};
}
trade::CResult CSimulatedCore::Advance(std::int64_t nNow)
{
	if (!m_bReady)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	bool bNeedsAdvance = false;
	int nPrevious = 0;
	for (const auto& entry : m_state.m_calendar)
	{
		if ((entry.second.m_nOpen <= nNow && nPrevious > m_state.m_nSettledDay) || (entry.second.m_nClose <= nNow && entry.first > m_state.m_nClosedDay))
		{
			bNeedsAdvance = true;
			break;
		}
		nPrevious = entry.first;
	}
	if (!bNeedsAdvance)
	{
		return {};
	}
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	bool bChanged = false;
	int nPreviousDay = 0;
	for (const auto& item : m_state.m_calendar)
	{
		const trade::CCalendar& day = item.second;
		if ((day.m_nOpen <= nNow) && (nPreviousDay > next.m_nSettledDay))
		{
			for (auto& entry : next.m_positions)
			{
				trade::CPosition& position = entry.second;
				edit.Save(&trade::CState::m_positions, entry.first);
				position.m_nAvailable += position.m_nToday;
				position.m_nToday = 0;
				trade::COrder order;
				order.m_accountIndex = position.m_accountIndex;
				order.m_strSecurity = position.m_strSecurity;
				Emit(next, trade::EventTy::Position, order);
			}
			next.m_nSettledDay = nPreviousDay;
			bChanged = true;
		}
		if ((day.m_nClose <= nNow) && (day.m_nDay > next.m_nClosedDay))
		{
			for (auto& entry : next.m_orders)
			{
				if (trade::IsActive(entry.second.m_status) && entry.second.m_nDay <= day.m_nDay)
				{
					Cancel(edit, entry.second, trade::Status::Expired, entry.second.m_requestId);
				}
			}
			next.m_nClosedDay = day.m_nDay;
			bChanged = true;
		}
		nPreviousDay = day.m_nDay;
	}
	return bChanged ? Commit(edit) : trade::CResult{};
}
trade::CCounterReply CSimulatedCore::Execute(const trade::CReqParam& request, std::int64_t nNow)
{
	trade::CCounterReply reply;
	reply.m_requestId = request.m_requestId;
	reply.m_correlationId = request.m_correlationId;
	reply.m_accountIndex = request.m_accountIndex;
	trade::Operation operation = request.GetOperation();
	reply.m_operation = operation;
	reply.m_nTime = nNow;
	reply.m_version = m_state.m_sequence;
	std::uint64_t nOrderId = 0;
	if (!IsAccountValid(request.m_accountIndex))
	{
		reply.m_result = { trade::Error::NotFound, "broker_id/account required" };
		return reply;
	}
	reply.m_result = Advance(nNow);
	if (!reply.m_result.Ok())
	{
		return reply;
	}
	if (!m_state.m_accounts.contains(request.m_accountIndex))
	{
		reply.m_result = { trade::Error::NotFound, "account not found" };
		return reply;
	}
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	trade::CResult result;
	if ((trade::Operation::Login == operation) || (trade::Operation::Connect == operation) || (trade::Operation::Disconnect == operation))
	{
		if (trade::Operation::Disconnect == operation)
		{
			for (const auto& item : next.m_orders)
			{
				if ((request.m_accountIndex == item.second.m_accountIndex) && trade::IsActive(item.second.m_status))
				{
					reply.m_result = Reject("active orders prevent disconnect");
					return reply;
				}
			}
		}
		edit.Save(&trade::CState::m_accounts, request.m_accountIndex);
		next.m_accounts.at(request.m_accountIndex).m_bConnected = trade::Operation::Disconnect != operation;
		trade::COrder order;
		order.m_accountIndex = request.m_accountIndex;
		order.m_requestId = request.m_requestId;
		Emit(next, trade::EventTy::Session, order);
	}
	else if (trade::Operation::Submit == operation)
	{
		trade::COrder order = std::get<trade::CSubmitParam>(request.m_param).m_order;
		order.m_accountIndex = request.m_accountIndex;
		order.m_requestId = request.m_requestId;
		result = Submit(edit, order, nNow, nOrderId);
		if (result.Ok())
		{
			auto depth = m_depths.find(order.m_strSecurity);
			if (m_depths.end() != depth)
			{
				trade::CDepth remaining = depth->second;
				result = Match(edit, remaining, nNow);
				if (result.Ok())
				{
					if (next.m_sequence != edit.Sequence())
					{
						edit.Save(&trade::CState::m_consumedBooks, remaining.m_strSecurity);
						next.m_consumedBooks[remaining.m_strSecurity] = { remaining.m_nTime, remaining.m_sequence };
					}
					result = Commit(edit);
					if (result.Ok())
					{
						depth->second = std::move(remaining);
					}
				}
				if (!result.Ok())
				{
					edit.Rollback();
				}
				reply.m_result = result;
				CompleteReply(reply, nOrderId);
				return reply;
			}
		}
	}
	else if (trade::Operation::Cancel == operation)
	{
		auto order = next.m_orders.find(std::get<trade::CCancelParam>(request.m_param).m_orderId);
		if ((next.m_orders.end() == order) || (request.m_accountIndex != order->second.m_accountIndex))
		{
			result = { trade::Error::NotFound, "order not found" };
		}
		else
		{
			Cancel(edit, order->second, trade::Status::Cancelled, request.m_requestId);
			nOrderId = std::get<trade::CCancelParam>(request.m_param).m_orderId;
		}
	}
	else
	{
		CompleteReply(reply, nOrderId);

		return reply;
	}
	if (!result.Ok())
	{
		edit.Rollback();
	}
	reply.m_result = result.Ok() ? Commit(edit) : result;
	CompleteReply(reply, nOrderId);
	return reply;
}
trade::CResult CSimulatedCore::OnDepth(trade::CDepth depth, std::int64_t nNow)
{
	if (!m_bMarketAvailable)
	{
		return { trade::Error::Unavailable, "market disconnected" };
	}
	if (depth.m_nTime < m_nStartTime)
	{
		return Reject("depth predates counter recovery");
	}
	trade::CResult result = Advance(nNow);
	if (!result.Ok())
	{
		return result;
	}
	const trade::CCalendar* currentDay = Calendar(nNow);
	if (nullptr == currentDay || !InSession(*currentDay, nNow) || !InSession(*currentDay, depth.m_nTime))
	{
		return Reject("depth outside continuous trading session");
	}
	auto consumed = m_state.m_consumedBooks.find(depth.m_strSecurity);
	if (m_state.m_consumedBooks.end() != consumed && std::make_pair(depth.m_nTime, depth.m_sequence) <= consumed->second)
	{
		return Reject("book liquidity already consumed before recovery");
	}
	auto previous = m_depths.find(depth.m_strSecurity);
	if ((0 == depth.m_sequence) || (depth.m_nTime > nNow) || (MaxAgeMs < nNow - depth.m_nTime) || ((m_depths.end() != previous) && ((previous->second.m_sequence >= depth.m_sequence) || (previous->second.m_nTime > depth.m_nTime))))
	{
		return Reject("duplicate, out-of-order or stale depth");
	}
	std::erase_if(depth.m_bids, [](const trade::CLevel& level)
	{
		return 0 == level.m_nQuantity;
	});
	std::erase_if(depth.m_asks, [](const trade::CLevel& level)
	{
		return 0 == level.m_nQuantity;
	});
	for (const auto& levels : { depth.m_bids, depth.m_asks })
	{
		for (const auto& level : levels)
		{
			if ((0 >= level.m_nPrice) || (0 > level.m_nQuantity))
			{
				return Reject("invalid depth level");
			}
		}
	}
	std::sort(depth.m_asks.begin(), depth.m_asks.end(), [](const trade::CLevel& left, const trade::CLevel& right)
	{
		return left.m_nPrice < right.m_nPrice;
	});
	std::sort(depth.m_bids.begin(), depth.m_bids.end(), [](const trade::CLevel& left, const trade::CLevel& right)
	{
		return left.m_nPrice > right.m_nPrice;
	});
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	result = Match(edit, depth, nNow);
	if (result.Ok() && (next.m_sequence != edit.Sequence()))
	{
		edit.Save(&trade::CState::m_consumedBooks, depth.m_strSecurity);
		next.m_consumedBooks[depth.m_strSecurity] = { depth.m_nTime, depth.m_sequence };
		result = Commit(edit);
	}
	if (result.Ok())
	{
		m_depths[depth.m_strSecurity] = std::move(depth);
	}
	return result;
}
void CSimulatedCore::ForgetDepth(const std::string& security)
{
	m_depths.erase(security);
}

void CSimulatedCore::OnMarketState(bool bAvailable)
{
	m_bMarketAvailable = bAvailable;
	if (!bAvailable)
	{
		m_depths.clear();
	}
}
trade::CAccountData CSimulatedCore::Snapshot(const _TyAccount& account, std::int64_t nNow) const
{
	trade::CAccountData data;
	data.m_account.m_accountIndex = account;
	data.m_version = m_state.m_sequence;
	data.m_nTime = nNow;
	auto iter = m_state.m_accounts.find(account);
	if (!IsAccountValid(account) || m_state.m_accounts.end() == iter)
	{
		return data;
	}
	data.m_account = iter->second;
	data.m_bReady = m_bReady;
	for (const auto& item : m_state.m_positions)
	{
		if (account == item.second.m_accountIndex)
		{
			data.m_positions.emplace_back(item.second);
		}
	}
	for (const auto& item : m_state.m_orders)
	{
		if (account == item.second.m_accountIndex)
		{
			data.m_orders.emplace_back(item.second);
		}
	}
	for (const auto& item : m_state.m_trades)
	{
		if (account == item.second.m_accountIndex)
		{
			data.m_trades.emplace_back(item.second);
		}
	}
	return data;
}
void CSimulatedCore::CompleteReply(trade::CCounterReply& reply, std::uint64_t nOrderId) const
{
	reply.m_version = m_state.m_sequence;
	if (!reply.m_result.Ok())
	{
		reply.m_data = std::monostate{};
		return;
	}
	switch (reply.m_operation)
	{
	case trade::Operation::Funds:
		reply.m_data = m_state.m_accounts.at(reply.m_accountIndex);
		break;
	case trade::Operation::Positions:
	{
		std::vector<trade::CPosition> rows;
		for (const auto& item : m_state.m_positions)
		{
			if (reply.m_accountIndex == item.second.m_accountIndex)
			{
				rows.emplace_back(item.second);
			}
		}
		reply.m_data = std::move(rows);
		break;
	}
	case trade::Operation::Orders:
	{
		std::vector<trade::COrder> rows;
		for (const auto& item : m_state.m_orders)
		{
			if (reply.m_accountIndex == item.second.m_accountIndex)
			{
				rows.emplace_back(item.second);
			}
		}
		reply.m_data = std::move(rows);
		break;
	}
	case trade::Operation::Trades:
	{
		std::vector<trade::CTrade> rows;
		for (const auto& item : m_state.m_trades)
		{
			if (reply.m_accountIndex == item.second.m_accountIndex)
			{
				rows.emplace_back(item.second);
			}
		}
		reply.m_data = std::move(rows);
		break;
	}
	default:
		reply.m_data = trade::COperationReceipt{ nOrderId };
		break;
	}
}
std::vector<trade::CCountEvent> CSimulatedCore::EventsAfter(std::uint64_t nSequence) const
{
	if (nSequence >= m_state.m_events.size())
	{
		return {};
	}
	return { m_state.m_events.begin() + static_cast<std::ptrdiff_t>(nSequence), m_state.m_events.end() };
}

std::vector<trade::CFunds> CSimulatedCore::Accounts() const
{
	std::vector<trade::CFunds> accounts;
	accounts.reserve(m_state.m_accounts.size());
	for (const auto& item : m_state.m_accounts)
	{
		accounts.emplace_back(item.second);
	}
	return accounts;
}
