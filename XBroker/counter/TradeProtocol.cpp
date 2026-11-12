#include "TradeProtocol.h"
#include <type_traits>
namespace trade::protocol
{
	void WriteOrder(const trade::COrder& row, request::TradeOrderRecord& wire)
	{
		SetAccountIdentity(row.m_accountIndex, wire);
		wire.set_client_order_id(row.m_clientId);
		wire.set_order_id(row.m_orderId);
		wire.set_request_id(row.m_requestId);
		wire.set_strategy_id(row.m_strategyId);
		wire.set_signal_id(row.m_signalId);
		wire.set_security(row.m_strSecurity);
		wire.set_side(static_cast<int>(row.m_side));
		wire.set_state(static_cast<int>(row.m_status));
		wire.set_price(row.m_nPrice);
		wire.set_price_scale(row.m_nPriceScale);
		wire.set_quantity(row.m_nQuantity);
		wire.set_filled_quantity(row.m_nFilled);
		wire.set_frozen(row.m_nFrozen);
		wire.set_turnover(row.m_nTurnover);
		wire.set_fee(row.m_nFee);
		wire.set_time_ms(row.m_nTime);
		wire.set_rule_version(row.m_strRuleVersion);
		wire.set_broker_order_id(row.m_strBrokerOrderId);
		wire.set_trading_day(row.m_nDay);
		wire.set_raw_status(row.m_strRawStatus);
		wire.set_broker_error(row.m_strError);
		wire.set_cancel_pending(row.m_bCancelPending);
		wire.set_extended_fields(true);
		wire.set_fee_known(row.m_bFeeKnown);
		wire.set_frozen_known(row.m_bFrozenKnown);
		wire.set_sdk_client_id(row.m_sdkClientId);
		wire.set_broker_time(row.m_nBrokerTime);
	}
	void WriteTrade(const trade::CTrade& row, request::TradeRecord& wire)
	{
		SetAccountIdentity(row.m_accountIndex, wire);
		wire.set_trade_id(row.m_tradeId);
		wire.set_order_id(row.m_orderId);
		wire.set_security(row.m_strSecurity);
		wire.set_side(static_cast<int>(row.m_side));
		wire.set_price(row.m_nPrice);
		wire.set_price_scale(row.m_nPriceScale);
		wire.set_quantity(row.m_nQuantity);
		wire.set_fee(row.m_nFee);
		wire.set_time_ms(row.m_nTime);
		wire.set_broker_trade_id(row.m_strBrokerTradeId);
		wire.set_broker_order_id(row.m_strBrokerOrderId);
		wire.set_trading_day(row.m_nDay);
		wire.set_report_index(row.m_reportIndex);
		wire.set_market(row.m_market);
		wire.set_extended_fields(true);
		wire.set_fee_known(row.m_bFeeKnown);
	}
	void WritePosition(const trade::CPosition& row, request::TradePositionRecord& wire)
	{
		SetAccountIdentity(row.m_accountIndex, wire);
		wire.set_security(row.m_strSecurity);
		wire.set_total_quantity(row.m_nTotal);
		wire.set_available_quantity(row.m_nAvailable);
		wire.set_frozen_quantity(row.m_nFrozen);
		wire.set_today_quantity(row.m_nToday);
		wire.set_book_cost_cents(row.m_nCost);
		wire.set_extended_fields(true);
		wire.set_frozen_known(row.m_bFrozenKnown);
		wire.set_cost_known(row.m_bCostKnown);
	}
	CSecurity Security(const std::string& strValue)
	{
		std::size_t nDot = strValue.rfind('.');
		return std::string::npos == nDot ? CSecurity{} : CSecurity(strValue.substr(0, nDot), ParseMarket(strValue.substr(nDot + 1)));
	}
	void WriteFunds(const CFunds& row, bool bReady, request::TradeAccountInfo& wire)
	{
		SetAccountIdentity(row.m_accountIndex, wire);
		wire.set_balance(row.m_nBalance);
		wire.set_frozen(row.m_nFrozen);
		wire.set_available(row.m_bExplicitAvailable ? row.m_nAvailable : row.m_nBalance - row.m_nFrozen);
		wire.set_extended_fields(true);
		wire.set_funds_valid(row.m_bFundsValid);
		wire.set_frozen_known(row.m_bFrozenKnown);
		wire.set_account_state(row.m_nState);
		wire.set_available_snapshot(bReady && row.m_bConnected && row.m_bFundsValid);
		wire.set_connected(row.m_bConnected);
	}

	void WriteReply(const CCounterReply& reply, ::CRequest& response)
	{
		request::TradeCounterReply wire;
		wire.set_request_id(reply.m_requestId);
		SetAccountIdentity(reply.m_accountIndex, wire);
		wire.set_operation(static_cast<int>(reply.m_operation));
		wire.set_error_code(static_cast<int>(reply.m_result.m_error));
		wire.set_error_message(reply.m_result.m_strMessage);
		wire.set_watermark(reply.m_version);
		wire.set_server_time_ms(reply.m_nTime);
		response.SetErrorInfo(static_cast<int>(reply.m_result.m_error), reply.m_result.m_strMessage);
		if (reply.m_result.Ok())
		{
			std::visit([&wire](const auto& value)
			{
				using T = std::decay_t<decltype(value)>;
				if constexpr (std::is_same_v<T, COperationReceipt>)
				{
					wire.mutable_receipt()->set_order_id(value.m_orderId);
				}
				else if constexpr (std::is_same_v<T, CFunds>)
				{
					WriteFunds(value, true, *wire.mutable_funds());
					wire.mutable_funds()->set_server_time_ms(wire.server_time_ms());
				}
				else if constexpr (std::is_same_v<T, std::vector<CPosition>>)
				{
					request::TradePositionList* rows = wire.mutable_positions();
					for (const auto& row : value)
					{
						WritePosition(row, *rows->add_positions());
					}
				}
				else if constexpr (std::is_same_v<T, std::vector<COrder>>)
				{
					request::TradeOrderList* rows = wire.mutable_orders();
					for (const auto& row : value)
					{
						WriteOrder(row, *rows->add_orders());
					}
				}
				else if constexpr (std::is_same_v<T, std::vector<CTrade>>)
				{
					request::TradeList* rows = wire.mutable_trades();
					for (const auto& row : value)
					{
						WriteTrade(row, *rows->add_trades());
					}
				}
			}, reply.m_data);
		}
		response.SetData(wire);
	}
	void WriteEvent(const CCountEvent& event, request::TradeEventRecord& wire)
	{
		wire.set_sequence(event.m_sequence);
		wire.set_request_id(event.m_requestId);
		SetAccountIdentity(event.m_accountIndex, wire);
		wire.set_kind(static_cast<int>(event.m_kind));
		wire.set_connected(event.m_funds.m_bConnected);
		wire.set_extended_fields(true);
		wire.set_account_valid(event.m_bAccountValid);
		if (event.m_bAccountValid)
		{
			WriteFunds(event.m_funds, true, *wire.mutable_account_snapshot());
		}
		switch (event.m_kind)
		{
		case EventTy::Order:
			WriteOrder(event.m_order, *wire.mutable_order());
			break;
		case EventTy::Deal:
			WriteTrade(event.m_trade, *wire.mutable_trade());
			break;
		case EventTy::Position:
			WritePosition(event.m_position, *wire.mutable_position());
			break;
		case EventTy::Funds:
			WriteFunds(event.m_funds, true, *wire.mutable_funds());
			break;
		default:
			break;
		}
	}
	request::TradeAccountRecord AccountPayload(const CTradeAccountInfo& info)
	{
		request::TradeAccountRecord payload;
		SetAccountIdentity(AccountIndex(info), payload);
		payload.set_site(info.m_strSite);
		payload.set_broker_name(info.m_strBrokerName);
		payload.set_name(info.m_strName);
		payload.set_state(static_cast<request::TradeAccountState>(info.m_state));
		payload.set_last_error(info.m_strLastError);
		return payload;
	}

	::CRequest Push(const CCountEvent& event, const CTradeAccountInfo& account)
	{
		::CRequest push;
		push.SetId(0);
		push.SetExtraData("sequence", std::to_string(event.m_sequence));
		push.SetExtraData("request_id", std::to_string(event.m_requestId));
		push.SetExtraData("broker_id", std::to_string(event.m_accountIndex.first));
		push.SetExtraData("account", event.m_accountIndex.second);
		switch (event.m_kind)
		{
		case trade::EventTy::Session:
			push.SetType(_TyRequest::TRADE_ACCOUNT_STATE_PUSH);
			push.SetData(AccountPayload(account));
			break;
		case trade::EventTy::Order:
		{
			push.SetType(_TyRequest::TRADE_ORDER_PUSH);
			request::TradeOrderList rows;
			WriteOrder(event.m_order, *rows.add_orders());
			push.SetData(rows);
		}
		break;
		case trade::EventTy::Deal:
		{
			push.SetType(_TyRequest::TRADE_TRADE_PUSH);
			request::TradeList rows;
			WriteTrade(event.m_trade, *rows.add_trades());
			push.SetData(rows);
		}
		break;
		case trade::EventTy::Funds:
		{
			push.SetType(_TyRequest::TRADE_ACCOUNT_INFO_PUSH);
			request::TradeAccountInfo rows;
			WriteFunds(event.m_funds, true, rows);
			push.SetData(rows);
		}
		break;
		case trade::EventTy::Position:
		{
			push.SetType(_TyRequest::TRADE_POSITION_PUSH);
			request::TradePositionList rows;
			WritePosition(event.m_position, *rows.add_positions());
			push.SetData(rows);
		}
		break;
		}
		if (event.m_bAccountValid)
		{
			request::TradeAccountInfo funds;
			WriteFunds(event.m_funds, true, funds);
			push.SetEventAccount(funds);
		}
		return push;
	}
} // namespace trade::protocol
