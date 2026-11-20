#include "ZhongTaiTypes.h"
#include "ZhongTaiConvert.h"
#ifdef XBROKER_ENABLE_XTP
#include <xtp_trader_api.h>
#include <atomic>
#include <cstring>
#include <cmath>
#include <mutex>
#include <stdexcept>
namespace trade::zhongtai
{
	namespace
	{
		std::string Security(const char* ticker, XTP_MARKET_TYPE market)
		{
			if (XTP_MKT_SH_A != market && XTP_MKT_SZ_A != market)
			{
				throw std::runtime_error("unsupported XTP market");
			}
			return std::string(ticker) + (XTP_MKT_SH_A == market ? ".SSE" : ".SZSE");
		}
		COrder Order(const XTPOrderInfo& w)
		{
			COrder r;
			r.m_broker_order_id = std::to_string(w.order_xtp_id);
			r.m_sdk_client_id = w.order_client_id;
			r.m_security = Security(w.ticker, w.market);
			r.m_price = Scaled(w.price, 4);
			r.m_price_scale = 4;
			r.m_quantity = w.quantity;
			r.m_filled = w.qty_traded;
			r.m_turnover = Scaled(w.trade_amount, 2);
			r.m_side = XTP_SIDE_BUY == w.side ? Side::Buy : Side::Sell;
			r.m_broker_time = w.update_time;
			r.m_time = BrokerTime(w.update_time);
			r.m_day = static_cast<int>(w.insert_time / 1000000000LL);
			r.m_fee_known = false;
			r.m_frozen_known = false;
			r.m_raw_status = std::to_string(w.order_status) + ":" + std::to_string(w.order_submit_status);
			switch (w.order_status)
			{
			case XTP_ORDER_STATUS_ALLTRADED:
				r.m_status = Status::Filled;
				break;
			case XTP_ORDER_STATUS_PARTTRADEDQUEUEING:
				r.m_status = Status::PartiallyFilled;
				break;
			case XTP_ORDER_STATUS_PARTTRADEDNOTQUEUEING:
			case XTP_ORDER_STATUS_CANCELED:
				r.m_status = Status::Cancelled;
				break;
			case XTP_ORDER_STATUS_NOTRADEQUEUEING:
				r.m_status = Status::Accepted;
				break;
			case XTP_ORDER_STATUS_REJECTED:
				r.m_status = Status::Rejected;
				break;
			case XTP_ORDER_STATUS_INIT:
				r.m_status = Status::Pending;
				break;
			default:
				r.m_status = Status::Unknown;
				break;
			}
			return r;
		}
		CTrade Trade(const XTPTradeReport& w)
		{
			CTrade r;
			r.m_broker_order_id = std::to_string(w.order_xtp_id);
			r.m_broker_trade_id = w.exec_id;
			r.m_report_index = w.report_index;
			r.m_market = static_cast<int>(w.market);
			r.m_security = Security(w.ticker, w.market);
			r.m_price = Scaled(w.price, 4);
			r.m_price_scale = 4;
			r.m_quantity = w.quantity;
			r.m_time = BrokerTime(w.trade_time);
			r.m_day = static_cast<int>(w.trade_time / 1000000000LL);
			r.m_side = XTP_SIDE_BUY == w.side ? Side::Buy : Side::Sell;
			r.m_fee_known = false;
			return r;
		}
		struct CRelease
		{
			void operator()(XTP::API::TraderApi* api) const
			{
				if (nullptr != api)
				{
					api->Release();
				}
			}
		};
	} // namespace
	class CZhongTaiApi final : public IZhongTaiApi, private XTP::API::TraderSpi
	{
	public:
		explicit CZhongTaiApi(const CConfig& config)
		{
			static std::atomic_flag created = ATOMIC_FLAG_INIT;
			if (created.test_and_set())
			{
				throw std::runtime_error("XTP allows one API creation per process");
			}
			m_api.reset(XTP::API::TraderApi::CreateTraderApi(static_cast<std::uint8_t>(config.m_client_id), config.m_log_path.c_str(), XTP_LOG_LEVEL_INFO));
			if (nullptr == m_api || 0 != std::strncmp(m_api->GetApiVersion(), "2.2.50.8", 8))
			{
				throw std::runtime_error("XTP SDK version must be 2.2.50.8");
			}
			m_client_id = static_cast<std::uint8_t>(config.m_client_id);
			m_api->SetSoftwareKey(config.m_software_key.c_str());
			m_api->SetSoftwareVersion(config.m_software_version.c_str());
			m_api->SetHeartBeatInterval(15);
			m_api->SubscribePublicTopic(XTP_TERT_RESTART);
			m_api->RegisterSpi(this);
		}
		~CZhongTaiApi() override
		{
			Stop();
		}
		void SetHandler(_TyHandler&& handler) override
		{
			std::lock_guard<std::mutex> lock(m_handler_mutex);
			m_handler = std::move(handler);
		}
		void Stop() override
		{
			m_api.reset();
			SetHandler({});
		}
		CLoginReply Login(const CSite& site, const std::string& account, const std::string& password) override
		{
			CLoginReply r;
			r.m_session = m_api->Login(site.m_host.c_str(), site.m_port, account.c_str(), password.c_str(), XTP_PROTOCOL_TCP, site.m_local_ip.empty() ? nullptr : site.m_local_ip.c_str());
			if (0 == r.m_session)
			{
				r.m_result = LastError();
				std::string message = r.m_result.m_message;
				r.m_auth_failure = std::string::npos == message.find("connect") && std::string::npos == message.find("Connect") && std::string::npos == message.find("timeout") && std::string::npos == message.find("network") && std::string::npos == message.find("socket") && std::string::npos == message.find("连接") && std::string::npos == message.find("超时");
			}
			else
			{
				try
				{
					r.m_day = std::stoi(m_api->GetTradingDay());
				}
				catch (...)
				{
					m_api->Logout(r.m_session);
					r.m_session = 0;
					r.m_result = { Error::Unavailable, "invalid SDK trading day" };
					r.m_auth_failure = true;
				}
			}
			return r;
		}
		void Logout(std::uint64_t session) override
		{
			if (nullptr != m_api && 0 != session)
			{
				m_api->Logout(session);
			}
		}
		CResult Query(std::uint64_t session, Operation operation, int requestId, std::int64_t reference) override
		{
			int result = -1;
			if (Operation::Funds == operation)
			{
				result = m_api->QueryAsset(session, requestId);
			}
			else if (Operation::Positions == operation)
			{
				result = m_api->QueryPosition("", session, requestId);
			}
			else if (Operation::Orders == operation)
			{
				XTPQueryOrderByPageReq query{};
				query.req_count = 1000;
				query.reference = reference;
				result = m_api->QueryOrdersByPage(&query, session, requestId);
			}
			else if (Operation::Trades == operation)
			{
				XTPQueryTraderByPageReq query{};
				query.req_count = 1000;
				query.reference = reference;
				result = m_api->QueryTradesByPage(&query, session, requestId);
			}
			return 0 == result ? CResult{} : LastError();
		}
		CResult Submit(std::uint64_t session, const COrder& order, std::uint64_t& brokerId) override
		{
			XTPOrderInsertInfo request{};
			request.order_client_id = order.m_sdk_client_id;
			std::string ticker = order.m_security.substr(0, 6);
			std::memcpy(request.ticker, ticker.data(), ticker.size());
			request.market = std::string::npos != order.m_security.find(".SSE") ? XTP_MKT_SH_A : XTP_MKT_SZ_A;
			request.price = static_cast<double>(order.m_price) / std::pow(10.0, order.m_price_scale);
			request.quantity = order.m_quantity;
			request.price_type = XTP_PRICE_LIMIT;
			request.side = Side::Buy == order.m_side ? XTP_SIDE_BUY : XTP_SIDE_SELL;
			request.business_type = XTP_BUSINESS_TYPE_CASH;
			brokerId = m_api->InsertOrder(&request, session);
			return 0 != brokerId ? CResult{} : LastError();
		}
		CResult Cancel(std::uint64_t session, std::uint64_t brokerId) override
		{
			return 0 != m_api->CancelOrder(brokerId, session) ? CResult{} : LastError();
		}

	private:
		CResult LastError()
		{
			XTPRI* error = m_api->GetApiLastError();
			return { Error::Unavailable, nullptr == error ? "XTP API call failed" : std::to_string(error->error_id) + ":" + error->error_msg };
		}
		void Emit(CApiEvent event)
		{
			_TyHandler handler;
			{
				std::lock_guard<std::mutex> lock(m_handler_mutex);
				handler = m_handler;
			}
			if (nullptr != handler)
			{
				handler(std::move(event));
			}
		}
		template <class _TyConvert>
		void Receive(ApiEventType type, std::uint64_t session, int requestId, bool last, XTPRI* error, _TyConvert&& convert)
		{
			CApiEvent event;
			event.m_type = type;
			event.m_session = session;
			event.m_request_id = requestId;
			event.m_last = last;
			try
			{
				convert(event);
				if (nullptr != error && 0 != error->error_id)
				{
					event.m_error_code = error->error_id;
					event.m_result = { Error::Rejected, std::to_string(error->error_id) + ":" + error->error_msg };
				}
			}
			catch (const std::exception& exception)
			{
				event.m_result = { Error::Unavailable, exception.what() };
			}
			Emit(std::move(event));
		}
		void OnDisconnected(std::uint64_t session, int reason) override
		{
			CApiEvent e;
			e.m_type = ApiEventType::Disconnected;
			e.m_session = session;
			e.m_result = { Error::Unavailable, "XTP disconnected: " + std::to_string(reason) };
			Emit(std::move(e));
		}
		void OnError(XTPRI* error) override
		{
			Receive(ApiEventType::Error, 0, 0, true, error, [](CApiEvent&)
			{
			});
		}
		void OnOrderEvent(XTPOrderInfo* order, XTPRI* error, std::uint64_t session) override
		{
			Receive(ApiEventType::Order, session, 0, true, nullptr, [&](CApiEvent& e)
			{
				if (nullptr != order)
				{
					e.m_order = Order(*order);
					if (m_client_id != m_api->GetClientIDByXTPID(order->order_xtp_id))
					{
						e.m_order.m_sdk_client_id = 0;
					}
					e.m_has_row = true;
					if (nullptr != error)
					{
						e.m_order.m_error_message = error->error_msg;
					}
				}
			});
		}
		void OnUnknownOrder(std::uint64_t orderId, std::uint64_t session) override
		{
			CApiEvent event;
			event.m_type = ApiEventType::UnknownOrder;
			event.m_session = session;
			event.m_cancel_order = orderId;
			Emit(std::move(event));
		}
		void OnTradeEvent(XTPTradeReport* trade, std::uint64_t session) override
		{
			Receive(ApiEventType::Trade, session, 0, true, nullptr, [&](CApiEvent& e)
			{
				if (nullptr != trade)
				{
					e.m_trade = Trade(*trade);
					e.m_has_row = true;
				}
			});
		}
		void OnCancelOrderError(XTPOrderCancelInfo* cancel, XTPRI* error, std::uint64_t session) override
		{
			Receive(ApiEventType::CancelError, session, 0, true, error, [&](CApiEvent& e)
			{
				if (nullptr != cancel)
				{
					e.m_cancel_order = cancel->order_xtp_id;
				}
			});
		}
		void OnQueryAsset(XTPQueryAssetRsp* asset, XTPRI* error, int requestId, bool last, std::uint64_t session) override
		{
			Receive(ApiEventType::Funds, session, requestId, last, error, [&](CApiEvent& e)
			{
				if (nullptr != asset)
				{
					if (XTP_ACCOUNT_NORMAL != asset->account_type)
					{
						throw std::runtime_error("only ordinary stock accounts supported");
					}
					e.m_has_row = true;
					e.m_funds.m_balance = Scaled(asset->total_asset - asset->security_asset, 2);
					e.m_funds.m_available = Scaled(asset->buying_power, 2);
					e.m_funds.m_frozen = Scaled(asset->withholding_amount, 2);
					e.m_funds.m_explicit_available = true;
				}
			});
		}
		void OnQueryPosition(XTPQueryStkPositionRsp* position, XTPRI* error, int requestId, bool last, std::uint64_t session) override
		{
			Receive(ApiEventType::Position, session, requestId, last, error, [&](CApiEvent& e)
			{
				if (nullptr != position)
				{
					e.m_has_row = true;
					e.m_position.m_security = Security(position->ticker, position->market);
					e.m_position.m_total = position->total_qty;
					e.m_position.m_available = position->sellable_qty;
					e.m_position.m_frozen_known = false;
					e.m_position.m_cost_known = false;
				}
			});
		}
		void OnQueryOrderByPage(XTPQueryOrderRsp* row, std::int64_t count, std::int64_t sequence, std::int64_t reference, int requestId, bool last, std::uint64_t session) override
		{
			Receive(ApiEventType::Order, session, requestId, last, nullptr, [&](CApiEvent& e)
			{
				e.m_reference = reference;
				if (nullptr != row && 0 != row->order_xtp_id)
				{
					e.m_order = Order(*row);
					if (m_client_id != m_api->GetClientIDByXTPID(row->order_xtp_id))
					{
						e.m_order.m_sdk_client_id = 0;
					}
					e.m_has_row = true;
				}
			});
		}
		void OnQueryTradeByPage(XTPQueryTradeRsp* row, std::int64_t count, std::int64_t sequence, std::int64_t reference, int requestId, bool last, std::uint64_t session) override
		{
			Receive(ApiEventType::Trade, session, requestId, last, nullptr, [&](CApiEvent& e)
			{
				e.m_reference = reference;
				if (nullptr != row && 0 != row->report_index)
				{
					e.m_trade = Trade(*row);
					e.m_has_row = true;
				}
			});
		}
		std::unique_ptr<XTP::API::TraderApi, CRelease> m_api;
		std::uint8_t m_client_id{ 0 };
		std::mutex m_handler_mutex;
		_TyHandler m_handler;
	};
	std::unique_ptr<IZhongTaiApi> CreateZhongTaiApi(const CConfig& config)
	{
		return std::make_unique<CZhongTaiApi>(config);
	}
} // namespace trade::zhongtai
#else
namespace trade::zhongtai
{
	std::unique_ptr<IZhongTaiApi> CreateZhongTaiApi(const CConfig&)
	{
		return nullptr;
	}
} // namespace trade::zhongtai
#endif
