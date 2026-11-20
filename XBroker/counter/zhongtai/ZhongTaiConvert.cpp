#include "ZhongTaiConvert.h"
#include <cmath>
#include <limits>
#include <stdexcept>
namespace trade::zhongtai
{
	std::int64_t Now()
	{
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	}
	std::int64_t Scaled(double value, int scale)
	{
		long double scaled = static_cast<long double>(value) * std::pow(10.0L, scale);
		if (!std::isfinite(scaled) || scaled > std::numeric_limits<std::int64_t>::max() || scaled < std::numeric_limits<std::int64_t>::min())
		{
			throw std::overflow_error("XTP numeric overflow");
		}
		return static_cast<std::int64_t>(std::round(scaled));
	}
	std::int64_t BrokerTime(std::int64_t value)
	{
		if (0 == value)
		{
			return 0;
		}
		std::chrono::year_month_day date{ std::chrono::year(static_cast<int>(value / 10000000000000LL)), std::chrono::month(static_cast<unsigned>(value / 100000000000LL % 100)), std::chrono::day(static_cast<unsigned>(value / 1000000000LL % 100)) };
		if (!date.ok())
		{
			throw std::runtime_error("invalid XTP timestamp");
		}
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::sys_days(date).time_since_epoch()).count() + (value / 10000000 % 100) * 3600000 + (value / 100000 % 100) * 60000 + (value / 1000 % 100) * 1000 + value % 1000 - 8 * 3600000;
	}
	bool IsStock(const std::string& security)
	{
		if (10 != security.size() && 11 != security.size())
		{
			return false;
		}
		for (std::size_t index = 0; index < 6; ++index)
		{
			if ('0' > security[index] || '9' < security[index])
			{
				return false;
			}
		}
		return (".SSE" == security.substr(6) && ("600" == security.substr(0, 3) || "601" == security.substr(0, 3) || "603" == security.substr(0, 3) || "605" == security.substr(0, 3) || "688" == security.substr(0, 3))) || (".SZSE" == security.substr(6) && ("000" == security.substr(0, 3) || "001" == security.substr(0, 3) || "002" == security.substr(0, 3) || "003" == security.substr(0, 3) || "300" == security.substr(0, 3) || "301" == security.substr(0, 3)));
	}
	COrder ReadOrder(const request::TradeOrderRecord& w)
	{
		COrder r;
		r.m_account_index = AccountIndex(w);
		r.m_order_id = w.order_id();
		r.m_client_id = w.client_order_id();
		r.m_request_id = w.request_id();
		r.m_strategy_id = w.strategy_id();
		r.m_signal_id = w.signal_id();
		r.m_security = w.security();
		r.m_side = static_cast<Side>(w.side());
		r.m_status = static_cast<Status>(w.state());
		r.m_price = w.price();
		r.m_price_scale = w.price_scale();
		r.m_quantity = w.quantity();
		r.m_filled = w.filled_quantity();
		r.m_frozen = w.frozen();
		r.m_turnover = w.turnover();
		r.m_fee = w.fee();
		r.m_time = w.time_ms();
		r.m_rule_version = w.rule_version();
		r.m_day = w.trading_day();
		r.m_broker_order_id = w.broker_order_id();
		r.m_raw_status = w.raw_status();
		r.m_error_message = w.broker_error();
		r.m_cancel_pending = w.cancel_pending();
		r.m_fee_known = !w.extended_fields() || w.fee_known();
		r.m_frozen_known = !w.extended_fields() || w.frozen_known();
		r.m_sdk_client_id = w.sdk_client_id();
		r.m_broker_time = w.broker_time();
		return r;
	}
	CTrade ReadTrade(const request::TradeRecord& w)
	{
		CTrade r;
		r.m_account_index = AccountIndex(w);
		r.m_trade_id = w.trade_id();
		r.m_order_id = w.order_id();
		r.m_security = w.security();
		r.m_side = static_cast<Side>(w.side());
		r.m_price = w.price();
		r.m_price_scale = w.price_scale();
		r.m_quantity = w.quantity();
		r.m_fee = w.fee();
		r.m_time = w.time_ms();
		r.m_day = w.trading_day();
		r.m_broker_trade_id = w.broker_trade_id();
		r.m_broker_order_id = w.broker_order_id();
		r.m_report_index = w.report_index();
		r.m_market = w.market();
		r.m_fee_known = !w.extended_fields() || w.fee_known();
		return r;
	}
	bool SameOrder(const COrder& a, const COrder& b)
	{
		return a.m_security == b.m_security && a.m_side == b.m_side && a.m_price == b.m_price && a.m_price_scale == b.m_price_scale && a.m_quantity == b.m_quantity && a.m_strategy_id == b.m_strategy_id && a.m_signal_id == b.m_signal_id;
	}
	std::string TradeKey(const CTrade& t)
	{
		return std::to_string(t.m_day) + ":" + std::to_string(t.m_market) + ":" + std::to_string(t.m_report_index);
	}
} // namespace trade::zhongtai
