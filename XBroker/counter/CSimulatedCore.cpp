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
		if (!Rate(nTurnover, rule.m_commission_ppm, nCommission) || !Rate(nTurnover, rule.m_transfer_ppm, nTransfer) || !Rate(nTurnover, trade::Side::Sell == side ? rule.m_sell_tax_ppm : 0, nTax))
		{
			return false;
		}
		return Add(std::max(nCommission, rule.m_minimum_fee), nTransfer, nFee) && Add(nFee, nTax, nFee);
	}
	bool InSession(const trade::CCalendar& day, std::int64_t nNow)
	{
		return ((day.m_open_time_ms <= nNow) && (nNow < day.m_break_time_ms)) || ((day.m_resume_time_ms <= nNow) && (nNow < day.m_close_time_ms));
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
	m_ready = result.Ok();
	if (!m_ready)
	{
		m_store->Release();
		return result;
	}
	mSourceDepths.clear();
	m_depths.clear();
	m_start_time = nNow;
	m_market_available = false;
	// A restored session is not a live transport connection.
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	for (auto& item : next.m_accounts)
	{
		edit.Save(&trade::CState::m_accounts, item.first);
		item.second.m_connected = false;
	}
	result = Commit(edit);
	return result.Ok() ? Advance(nNow) : result;
}
void CSimulatedCore::Stop()
{
	m_ready = false;
	m_market_available = false;
	mSourceDepths.clear();
	m_depths.clear();
	m_store->Release();
}
trade::CResult CSimulatedCore::Renew()
{
	if (!m_ready)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	trade::CResult result = m_store->Renew();
	if (!result.Ok())
	{
		m_ready = false;
		mSourceDepths.clear();
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
	return m_ready;
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
		auto order = state.m_orders.find(trade.m_order_id);
		if (item.first != trade.m_trade_id || state.m_next_trade <= item.first || state.m_orders.end() == order || order->second.m_account_index != trade.m_account_index || order->second.m_security != trade.m_security || order->second.m_side != trade.m_side || 0 >= trade.m_quantity || 0 > trade.m_fee || !Money(trade.m_price, trade.m_quantity, trade.m_price_scale, nMoney) || !Add(filled[trade.m_order_id], trade.m_quantity, filled[trade.m_order_id]) || !Add(turnover[trade.m_order_id], nMoney, turnover[trade.m_order_id]) || !Add(fees[trade.m_order_id], trade.m_fee, fees[trade.m_order_id]))
		{
			return Reject("trade journal invariant failed");
		}
	}

	for (const auto& item : state.m_rules)
	{
		const trade::CRule& rule = item.second;
		if (!state.m_calendar.contains(rule.m_day) || 0 >= rule.m_tick || 0 >= rule.m_buy_step || 0 >= rule.m_sell_step || 0 >= rule.m_buy_minimum || 0 >= rule.m_sell_minimum || 0 >= rule.m_lower || rule.m_upper < rule.m_lower || 0 > rule.m_price_scale || 6 < rule.m_price_scale || 0 > rule.m_commission_ppm || 1000000 < rule.m_commission_ppm || 0 > rule.m_minimum_fee || 0 > rule.m_sell_tax_ppm || 1000000 < rule.m_sell_tax_ppm || 0 > rule.m_transfer_ppm || 1000000 < rule.m_transfer_ppm)
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
		if (0 == order.m_client_id || 0 >= order.m_quantity || 0 >= order.m_price || trade::Status::Accepted > order.m_status || trade::Status::Rejected < order.m_status || (trade::Side::Buy != order.m_side && trade::Side::Sell != order.m_side) || filled[item.first] != order.m_filled || turnover[item.first] != order.m_turnover || fees[item.first] != order.m_fee)
		{
			return Reject("order/trade journal mismatch");
		}

		if ((item.first != order.m_order_id) || (state.m_next_order <= order.m_order_id) || !state.m_accounts.contains(order.m_account_index) || !clients.emplace(order.m_account_index, order.m_client_id).second || (0 > order.m_filled) || (order.m_filled > order.m_quantity) || (0 > order.m_frozen))
		{
			return Reject("invalid persisted order");
		}
		if (trade::IsActive(order.m_status))
		{
			if (trade::Side::Buy == order.m_side)
			{
				if (!Add(frozen[order.m_account_index], order.m_frozen, frozen[order.m_account_index]))
				{
					return Reject("frozen overflow");
				}
			}
			else
			{
				if (!Add(shares[{ order.m_account_index, order.m_security }], order.m_quantity - order.m_filled, shares[{ order.m_account_index, order.m_security }]))
				{
					return Reject("share overflow");
				}
			}
		}
		else if (0 != order.m_frozen)
		{
			return Reject("terminal order still frozen");
		}
	}
	for (const auto& item : state.m_accounts)
	{
		if ((item.first != item.second.m_account_index) || (0 > item.second.m_balance) || (0 > item.second.m_frozen) || (item.second.m_balance < item.second.m_frozen) || (frozen[item.first] != item.second.m_frozen))
		{
			return Reject("cash ledger invariant failed");
		}
	}
	for (const auto& item : state.m_positions)
	{
		const trade::CPosition& position = item.second;
		std::int64_t nTotal = 0;
		if (!state.m_accounts.contains(position.m_account_index) || !Add(position.m_available, position.m_frozen, nTotal) || !Add(nTotal, position.m_today, nTotal) || (nTotal != position.m_total) || (position.m_frozen != shares[item.first]) || (0 > position.m_cost))
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
			m_ready = false;
			mSourceDepths.clear();
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
	if (!m_ready)
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
	order.m_account_index = account;
	Emit(next, trade::EventTy::Funds, order);
	return Commit(edit);
}
trade::CResult CSimulatedCore::ImportRules(const std::vector<trade::CRule>& rules, const std::vector<trade::CCalendar>& calendar)
{
	if (!m_ready)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	for (const auto& day : calendar)
	{
		if ((0 >= day.m_day) || !(day.m_open_time_ms < day.m_break_time_ms && day.m_break_time_ms < day.m_resume_time_ms && day.m_resume_time_ms < day.m_close_time_ms))
		{
			return Reject("invalid calendar");
		}
		auto existing = next.m_calendar.find(day.m_day);
		// Existing calendar entries cannot be changed after orders have been accepted.
		if ((next.m_calendar.end() != existing) && !next.m_orders.empty() && ((existing->second.m_open_time_ms != day.m_open_time_ms) || (existing->second.m_break_time_ms != day.m_break_time_ms) || (existing->second.m_resume_time_ms != day.m_resume_time_ms) || (existing->second.m_close_time_ms != day.m_close_time_ms)))
		{
			return Reject("calendar immutable after trading");
		}
		edit.Save(&trade::CState::m_calendar, day.m_day);
		next.m_calendar[day.m_day] = day;
	}
	std::int64_t nPreviousClose = 0;
	for (const auto& entry : next.m_calendar)
	{
		if (entry.second.m_open_time_ms <= nPreviousClose)
		{
			return Reject("overlapping or unordered calendar");
		}
		nPreviousClose = entry.second.m_close_time_ms;
	}
	for (const auto& rule : rules)
	{
		if (rule.m_version_text.empty() || rule.m_security.empty() || !next.m_calendar.contains(rule.m_day) || (0 > rule.m_price_scale) || (6 < rule.m_price_scale) || (0 >= rule.m_tick) || (0 >= rule.m_buy_minimum) || (0 >= rule.m_buy_step) || (0 >= rule.m_sell_minimum) || (0 >= rule.m_sell_step) || (0 >= rule.m_lower) || (rule.m_lower > rule.m_upper) || (0 > rule.m_minimum_fee) || (0 > rule.m_commission_ppm) || (1000000 < rule.m_commission_ppm) || (0 > rule.m_sell_tax_ppm) || (1000000 < rule.m_sell_tax_ppm) || (0 > rule.m_transfer_ppm) || (1000000 < rule.m_transfer_ppm))
		{
			return Reject("invalid security rule");
		}
		bool bExchange = rule.m_security.ends_with(".SSE") || rule.m_security.ends_with(".SZSE");
		if (!bExchange || (("MAIN" != rule.m_board) && ("CHINEXT" != rule.m_board) && ("STAR" != rule.m_board)))
		{
			return { trade::Error::Unsupported, "unsupported security board" };
		}
		std::pair<int, std::string> key = std::make_pair(rule.m_day, rule.m_security);
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
		if ((item.second.m_open_time_ms <= nNow) && (nNow < item.second.m_close_time_ms))
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
	event.m_request_id = 0 == nRequestId ? order.m_request_id : nRequestId;
	event.m_account_index = order.m_account_index;
	event.m_kind = kind;
	event.m_order = order;
	event.m_trade = trade;
	event.m_funds = state.m_accounts.at(order.m_account_index);
	auto position = state.m_positions.find({ order.m_account_index, order.m_security });
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
		if ((previous.m_account_index == order.m_account_index) && ((previous.m_client_id == order.m_client_id) || ((0 != order.m_strategy_id) && (previous.m_strategy_id == order.m_strategy_id) && (previous.m_signal_id == order.m_signal_id))))
		{
			if ((previous.m_security != order.m_security) || (previous.m_side != order.m_side) || (previous.m_price != order.m_price) || (previous.m_quantity != order.m_quantity) || (previous.m_price_scale != order.m_price_scale) || (previous.m_strategy_id != order.m_strategy_id) || (previous.m_signal_id != order.m_signal_id))
			{
				return { trade::Error::Conflict, "client order ID reused with different parameters" };
			}
			nOrderId = previous.m_order_id;
			return {};
		}
	}
	const trade::CCalendar* day = Calendar(nNow);
	if ((nullptr == day) || !InSession(*day, nNow))
	{
		return { trade::Error::Unsupported, "only continuous trading sessions supported" };
	}
	auto ruleIter = state.m_rules.find({ day->m_day, order.m_security });
	if (state.m_rules.end() == ruleIter)
	{
		return Reject("missing security rules");
	}
	const trade::CRule& rule = ruleIter->second;
	if (rule.m_corporate_action)
	{
		return Reject("corporate action pending");
	}
	if ((0 == order.m_client_id) || (0 >= order.m_quantity) || (rule.m_price_scale != order.m_price_scale) || (rule.m_lower > order.m_price) || (rule.m_upper < order.m_price) || (0 != order.m_price % rule.m_tick))
	{
		return Reject("invalid limit order");
	}
	order.m_order_id = 0;
	order.m_filled = 0;
	order.m_frozen = 0;
	order.m_turnover = 0;
	order.m_fee = 0;
	order.m_status = trade::Status::Accepted;
	if ((trade::Side::Buy != order.m_side) && (trade::Side::Sell != order.m_side))
	{
		return Reject("invalid side");
	}
	edit.Save(&trade::CState::m_accounts, order.m_account_index);
	trade::CFunds& account = state.m_accounts.at(order.m_account_index);
	if (!account.m_connected)
	{
		return { trade::Error::Unavailable, "account disconnected" };
	}
	if (!m_market_available)
	{
		return { trade::Error::Unavailable, "market disconnected" };
	}
	std::int64_t nTurnover = 0, nFee = 0;
	if (!Money(order.m_price, order.m_quantity, order.m_price_scale, nTurnover) || !Fee(nTurnover, rule, order.m_side, nFee))
	{
		return Reject("money overflow or sub-cent turnover");
	}
	if (trade::Side::Sell == order.m_side && nTurnover < nFee)
	{
		return Reject("sell proceeds do not cover order fees");
	}
	if (trade::Side::Buy == order.m_side)
	{
		if ((order.m_quantity < rule.m_buy_minimum) || (0 != (order.m_quantity - rule.m_buy_minimum) % rule.m_buy_step) || !Add(nTurnover, nFee, order.m_frozen) || (order.m_frozen > account.m_balance - account.m_frozen))
		{
			return Reject("invalid buy size or insufficient funds");
		}
		account.m_frozen += order.m_frozen;
	}
	else
	{
		auto position = state.m_positions.find({ order.m_account_index, order.m_security });
		if ((state.m_positions.end() == position) || (position->second.m_available < order.m_quantity) || (order.m_quantity < rule.m_sell_minimum) || (0 != (order.m_quantity - rule.m_sell_minimum) % rule.m_sell_step))
		{
			return Reject("invalid sell size or insufficient available position");
		}
		edit.Save(&trade::CState::m_positions, position->first);
		position->second.m_available -= order.m_quantity;
		position->second.m_frozen += order.m_quantity;
	}
	order.m_order_id = state.m_next_order++;
	order.m_day = day->m_day;
	order.m_time = nNow;
	order.m_rule_version = rule.m_version_text;
	nOrderId = order.m_order_id;
	edit.Save(&trade::CState::m_orders, order.m_order_id);
	state.m_orders.emplace(order.m_order_id, order);
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
	edit.Save(&trade::CState::m_orders, order.m_order_id);
	edit.Save(&trade::CState::m_accounts, order.m_account_index);
	if (trade::Side::Sell == order.m_side)
	{
		edit.Save(&trade::CState::m_positions, std::make_pair(order.m_account_index, order.m_security));
	}
	if (trade::Side::Buy == order.m_side)
	{
		state.m_accounts.at(order.m_account_index).m_frozen -= order.m_frozen;
	}
	else
	{
		trade::CPosition& position = state.m_positions.at({ order.m_account_index, order.m_security });
		position.m_available += order.m_quantity - order.m_filled;
		position.m_frozen -= order.m_quantity - order.m_filled;
	}
	order.m_frozen = 0;
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
	if (!Money(nPrice, nQuantity, order.m_price_scale, nMoney) || !Add(order.m_turnover, nMoney, nTurnover) || !Fee(nTurnover, rule, order.m_side, nTotalFee))
	{
		return Reject("fill arithmetic overflow");
	}
	std::int64_t nFee = nTotalFee - order.m_fee;
	edit.Save(&trade::CState::m_accounts, order.m_account_index);
	trade::CFunds& account = state.m_accounts.at(order.m_account_index);
	std::pair<_TyAccount, std::string> key = std::make_pair(order.m_account_index, order.m_security);
	edit.Save(&trade::CState::m_orders, order.m_order_id);
	edit.Save(&trade::CState::m_positions, key);
	trade::CPosition& position = state.m_positions[key];
	position.m_account_index = order.m_account_index;
	position.m_security = order.m_security;
	std::int64_t nRemaining = order.m_quantity - order.m_filled - nQuantity;
	if (trade::Side::Buy == order.m_side)
	{
		std::int64_t nDebit = 0, nRemainingMoney = 0, nWorstTurnover = 0, nWorstFee = 0, nReserve = 0, nRequired = 0;
		if (!Add(nMoney, nFee, nDebit) || !Money(order.m_price, nRemaining, order.m_price_scale, nRemainingMoney) || !Add(nTurnover, nRemainingMoney, nWorstTurnover) || !Fee(nWorstTurnover, rule, order.m_side, nWorstFee) || !Add(nRemainingMoney, nWorstFee - nTotalFee, nReserve) || (account.m_balance < nDebit) || !Add(nDebit, nReserve, nRequired) || (order.m_frozen < nRequired))
		{
			return Reject("invalid fill reserve");
		}
		account.m_balance -= nDebit;
		account.m_frozen -= order.m_frozen - nReserve;
		order.m_frozen = nReserve;
		if (!Add(position.m_total, nQuantity, position.m_total) || !Add(position.m_today, nQuantity, position.m_today) || !Add(position.m_cost, nDebit, position.m_cost))
		{
			return Reject("position overflow");
		}
	}
	else
	{
		if ((position.m_frozen < nQuantity) || (nMoney < nFee) || !Add(account.m_balance, nMoney - nFee, account.m_balance))
		{
			return Reject("invalid sell settlement");
		}
		// Proportional book cost, using quotient/remainder to avoid multiplication overflow.
		std::int64_t nCost = position.m_cost / position.m_total * nQuantity;
		std::int64_t nRemainder = position.m_cost % position.m_total;
		if ((0 != nQuantity) && (std::numeric_limits<std::int64_t>::max() / nQuantity < nRemainder))
		{
			return Reject("cost arithmetic overflow");
		}
		nCost += nRemainder * nQuantity / position.m_total;
		position.m_cost -= nCost;
		position.m_total -= nQuantity;
		position.m_frozen -= nQuantity;
	}
	order.m_filled += nQuantity;
	order.m_turnover = nTurnover;
	order.m_fee = nTotalFee;
	order.m_status = 0 == nRemaining ? trade::Status::Filled : trade::Status::PartiallyFilled;
	trade::CTrade trade{ state.m_next_trade++, order.m_order_id, order.m_account_index, order.m_security, order.m_side, nPrice, nQuantity, nFee, nNow, order.m_price_scale };
	edit.Save(&trade::CState::m_trades, trade.m_trade_id);
	state.m_trades.emplace(trade.m_trade_id, trade);
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
	if (!m_market_available || (nullptr == day) || !InSession(*day, nNow) || (0 == depth.m_time) || (depth.m_time > nNow) || (MaxAgeMs < nNow - depth.m_time))
	{
		return {};
	}
	auto ruleIter = state.m_rules.find({ day->m_day, depth.m_security });
	if (state.m_rules.end() == ruleIter)
	{
		return {};
	}
	const trade::CRule& rule = ruleIter->second;
	if (rule.m_corporate_action || (rule.m_price_scale != depth.m_price_scale))
	{
		return {};
	}
	for (const auto& level : depth.m_bids)
	{
		if (level.m_price < rule.m_lower || level.m_price > rule.m_upper || 0 != level.m_price % rule.m_tick)
		{
			return Reject("bid outside configured price rules");
		}
	}
	for (const auto& level : depth.m_asks)
	{
		if (level.m_price < rule.m_lower || level.m_price > rule.m_upper || 0 != level.m_price % rule.m_tick)
		{
			return Reject("ask outside configured price rules");
		}
	}
	std::vector<std::uint64_t> orders;
	for (const auto& item : state.m_orders)
	{
		if (trade::IsActive(item.second.m_status) && (item.second.m_day == day->m_day) && (item.second.m_security == depth.m_security) && state.m_accounts.at(item.second.m_account_index).m_connected)
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
		if (left.m_price != right.m_price)
		{
			return trade::Side::Buy == left.m_side ? left.m_price > right.m_price : left.m_price < right.m_price;
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
			if ((0 >= level.m_quantity) || (trade::Side::Buy == order.m_side ? level.m_price > order.m_price : level.m_price < order.m_price))
			{
				continue;
			}
			std::int64_t nQuantity = std::min(level.m_quantity, order.m_quantity - order.m_filled);
			trade::CResult result = Fill(edit, order, rule, level.m_price, nQuantity, nNow);
			if (!result.Ok())
			{
				return result;
			}
			level.m_quantity -= nQuantity;
		}
	}
	return {};
}
trade::CResult CSimulatedCore::Advance(std::int64_t nNow)
{
	if (!m_ready)
	{
		return { trade::Error::Unavailable, "counter not ready" };
	}
	bool bNeedsAdvance = false;
	int nPrevious = 0;
	for (const auto& entry : m_state.m_calendar)
	{
		if ((entry.second.m_open_time_ms <= nNow && nPrevious > m_state.m_settled_day) || (entry.second.m_close_time_ms <= nNow && entry.first > m_state.m_closed_day))
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
		if ((day.m_open_time_ms <= nNow) && (nPreviousDay > next.m_settled_day))
		{
			for (auto& entry : next.m_positions)
			{
				trade::CPosition& position = entry.second;
				edit.Save(&trade::CState::m_positions, entry.first);
				position.m_available += position.m_today;
				position.m_today = 0;
				trade::COrder order;
				order.m_account_index = position.m_account_index;
				order.m_security = position.m_security;
				Emit(next, trade::EventTy::Position, order);
			}
			next.m_settled_day = nPreviousDay;
			bChanged = true;
		}
		if ((day.m_close_time_ms <= nNow) && (day.m_day > next.m_closed_day))
		{
			for (auto& entry : next.m_orders)
			{
				if (trade::IsActive(entry.second.m_status) && entry.second.m_day <= day.m_day)
				{
					Cancel(edit, entry.second, trade::Status::Expired, entry.second.m_request_id);
				}
			}
			next.m_closed_day = day.m_day;
			bChanged = true;
		}
		nPreviousDay = day.m_day;
	}
	return bChanged ? Commit(edit) : trade::CResult{};
}
trade::CCounterReply CSimulatedCore::Execute(const trade::CReqParam& request, std::int64_t nNow)
{
	trade::CCounterReply reply;
	reply.m_request_id = request.m_request_id;
	reply.m_correlation_id = request.m_correlation_id;
	reply.m_account_index = request.m_account_index;
	trade::Operation operation = request.GetOperation();
	reply.m_operation = operation;
	reply.m_time = nNow;
	reply.m_version = m_state.m_sequence;
	std::uint64_t nOrderId = 0;
	if (!IsAccountValid(request.m_account_index))
	{
		reply.m_result = { trade::Error::NotFound, "broker_id/account required" };
		return reply;
	}
	reply.m_result = Advance(nNow);
	if (!reply.m_result.Ok())
	{
		return reply;
	}
	if (!m_state.m_accounts.contains(request.m_account_index))
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
				if ((request.m_account_index == item.second.m_account_index) && trade::IsActive(item.second.m_status))
				{
					reply.m_result = Reject("active orders prevent disconnect");
					return reply;
				}
			}
		}
		edit.Save(&trade::CState::m_accounts, request.m_account_index);
		next.m_accounts.at(request.m_account_index).m_connected = trade::Operation::Disconnect != operation;
		trade::COrder order;
		order.m_account_index = request.m_account_index;
		order.m_request_id = request.m_request_id;
		Emit(next, trade::EventTy::Session, order);
	}
	else if (trade::Operation::Submit == operation)
	{
		trade::COrder order = std::get<trade::CSubmitParam>(request.m_param).m_order;
		order.m_account_index = request.m_account_index;
		order.m_request_id = request.m_request_id;
		result = Submit(edit, order, nNow, nOrderId);
		if (result.Ok())
		{
			auto depth = m_depths.find(order.m_security);
			if (m_depths.end() != depth)
			{
				trade::CDepth remaining = depth->second;
				std::uint64_t sequenceBeforeMatch = next.m_sequence;
				result = Match(edit, remaining, nNow);
				if (result.Ok())
				{
					if (next.m_sequence != sequenceBeforeMatch)
					{
						edit.Save(&trade::CState::m_consumed_books, remaining.m_security);
						next.m_consumed_books[remaining.m_security] = { remaining.m_time, remaining.m_sequence };
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
		auto order = next.m_orders.find(std::get<trade::CCancelParam>(request.m_param).m_order_id);
		if ((next.m_orders.end() == order) || (request.m_account_index != order->second.m_account_index))
		{
			result = { trade::Error::NotFound, "order not found" };
		}
		else
		{
			Cancel(edit, order->second, trade::Status::Cancelled, request.m_request_id);
			nOrderId = std::get<trade::CCancelParam>(request.m_param).m_order_id;
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
	if (!m_market_available)
	{
		return { trade::Error::Unavailable, "market disconnected" };
	}
	if (depth.m_bids.empty() && depth.m_asks.empty())
	{
		auto previous = m_depths.find(depth.m_security);
		if ((m_depths.end() != previous) && (previous->second.m_sequence < depth.m_sequence))
		{
			previous->second.m_time = 0;
			previous->second.m_sequence = depth.m_sequence;
		}
		return {};
	}
	if (depth.m_time < m_start_time)
	{
		return Reject("depth predates counter recovery");
	}
	trade::CResult result = Advance(nNow);
	if (!result.Ok())
	{
		return result;
	}
	const trade::CCalendar* currentDay = Calendar(nNow);
	if (nullptr == currentDay || !InSession(*currentDay, nNow) || !InSession(*currentDay, depth.m_time))
	{
		return Reject("depth outside continuous trading session");
	}
	auto consumed = m_state.m_consumed_books.find(depth.m_security);
	if (m_state.m_consumed_books.end() != consumed && std::make_pair(depth.m_time, depth.m_sequence) <= consumed->second)
	{
		return Reject("book liquidity already consumed before recovery");
	}
	auto previous = m_depths.find(depth.m_security);
	auto source = mSourceDepths.find(depth.m_security);
	if ((mSourceDepths.end() != source) && (source->second.m_time > depth.m_time))
	{
		return Reject("out-of-order depth time");
	}
	if ((0 == depth.m_sequence) || (depth.m_time > nNow) || (MaxAgeMs < nNow - depth.m_time) || ((m_depths.end() != previous) && ((previous->second.m_sequence >= depth.m_sequence) || (previous->second.m_time > depth.m_time))))
	{
		return Reject("duplicate, out-of-order or stale depth");
	}
	std::erase_if(depth.m_bids, [](const trade::CLevel& level)
	{
		return 0 == level.m_quantity;
	});
	std::erase_if(depth.m_asks, [](const trade::CLevel& level)
	{
		return 0 == level.m_quantity;
	});
	for (const auto& levels : { depth.m_bids, depth.m_asks })
	{
		for (const auto& level : levels)
		{
			if ((0 >= level.m_price) || (0 > level.m_quantity))
			{
				return Reject("invalid depth level");
			}
		}
	}
	std::sort(depth.m_asks.begin(), depth.m_asks.end(), [](const trade::CLevel& left, const trade::CLevel& right)
	{
		return left.m_price < right.m_price;
	});
	std::sort(depth.m_bids.begin(), depth.m_bids.end(), [](const trade::CLevel& left, const trade::CLevel& right)
	{
		return left.m_price > right.m_price;
	});
	trade::CDepth sourceDepth = depth;
	if ((m_depths.end() != previous) && (mSourceDepths.end() != source)
		&& InSession(*currentDay, source->second.m_time) && (source->second.m_price_scale == depth.m_price_scale))
	{
		// Carry forward consumption at each price across overlapping snapshots.
		auto preserveConsumption = [](std::vector<trade::CLevel>& levels, const std::vector<trade::CLevel>& original,
			const std::vector<trade::CLevel>& remaining)
		{
			for (auto& level : levels)
			{
				auto before = std::find_if(original.begin(), original.end(), [&level](const trade::CLevel& entry)
				{
					return level.m_price == entry.m_price;
				});
				if (original.end() != before)
				{
					std::size_t index = static_cast<std::size_t>(before - original.begin());
					level.m_quantity = std::max<std::int64_t>(0, level.m_quantity - (before->m_quantity - remaining[index].m_quantity));
				}
			}
		};
		preserveConsumption(depth.m_bids, source->second.m_bids, previous->second.m_bids);
		preserveConsumption(depth.m_asks, source->second.m_asks, previous->second.m_asks);
	}
	trade::CStateEdit edit(m_state);
	trade::CState& next = edit.State();
	result = Match(edit, depth, nNow);
	if (result.Ok() && (next.m_sequence != edit.Sequence()))
	{
		edit.Save(&trade::CState::m_consumed_books, depth.m_security);
		next.m_consumed_books[depth.m_security] = { depth.m_time, depth.m_sequence };
		result = Commit(edit);
	}
	if (result.Ok())
	{
		mSourceDepths[depth.m_security] = std::move(sourceDepth);
		m_depths[depth.m_security] = std::move(depth);
	}
	return result;
}
void CSimulatedCore::ForgetDepth(const std::string& security)
{
	auto previous = m_depths.find(security);
	if (m_depths.end() != previous)
	{
		previous->second.m_time = 0;
	}
}

void CSimulatedCore::OnMarketState(bool bAvailable)
{
	m_market_available = bAvailable;
	if (!bAvailable)
	{
		for (auto& item : m_depths)
		{
			item.second.m_time = 0;
			item.second.m_sequence = 0;
		}
	}
}
trade::CAccountData CSimulatedCore::Snapshot(const _TyAccount& account, std::int64_t nNow) const
{
	trade::CAccountData data;
	data.m_account.m_account_index = account;
	data.m_version = m_state.m_sequence;
	data.m_time = nNow;
	auto iter = m_state.m_accounts.find(account);
	if (!IsAccountValid(account) || m_state.m_accounts.end() == iter)
	{
		return data;
	}
	data.m_account = iter->second;
	data.m_ready = m_ready;
	for (const auto& item : m_state.m_positions)
	{
		if (account == item.second.m_account_index)
		{
			data.m_positions.emplace_back(item.second);
		}
	}
	for (const auto& item : m_state.m_orders)
	{
		if (account == item.second.m_account_index)
		{
			data.m_orders.emplace_back(item.second);
		}
	}
	for (const auto& item : m_state.m_trades)
	{
		if (account == item.second.m_account_index)
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
		reply.m_data = m_state.m_accounts.at(reply.m_account_index);
		break;
	case trade::Operation::Positions:
	{
		std::vector<trade::CPosition> rows;
		for (const auto& item : m_state.m_positions)
		{
			if (reply.m_account_index == item.second.m_account_index)
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
			if (reply.m_account_index == item.second.m_account_index)
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
			if (reply.m_account_index == item.second.m_account_index)
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
