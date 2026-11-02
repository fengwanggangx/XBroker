#include "CSimulatedDispatcher.h"
#include "../system/CSession.h"
#include "../common/utility.h"
#include "../request/request.pb.h"
#include "../request/RequestCenter.h"

bool CSimulatedDispatcher::OnMarketResponse(const CRequest& request)
{
	if (!request.GetData().has_depth())
	{
		return false;
	}
	const _TyDepthData& wire = request.GetData().depth();
	if (wire.stale() || (0 == wire.bids_size() + wire.asks_size()))
	{
		return false;
	}
	trade::CDepth depth;
	depth.m_strSecurity = wire.security().symbol() + "." + GetMarketString(static_cast<Exchange>(wire.security().exchange()));
	std::string strSequence = request.GetExtraData("sequence");
	if (!utility::to_number(strSequence, depth.m_sequence) || (0 == depth.m_sequence))
	{
		return false;
	}
	depth.m_nTime = wire.exchange_time_ms();
	depth.m_nPriceScale = 0 < wire.bids_size() ? wire.bids(0).price_scale() : wire.asks(0).price_scale();
	for (const auto& level : wire.bids())
	{
		if (level.price_scale() != depth.m_nPriceScale)
		{
			return false;
		}
		depth.m_bids.push_back({ level.price(), level.volume() });
	}
	for (const auto& level : wire.asks())
	{
		if (level.price_scale() != depth.m_nPriceScale)
		{
			return false;
		}
		depth.m_asks.push_back({ level.price(), level.volume() });
	}
	OnDepth(std::move(depth));
	return true;
}

void CSimulatedDispatcher::OnSessionState(SessionState state, const std::string&)
{
	OnMarketState(SessionState::Ready == state);
}

bool CSimulatedDispatcher::ChangeDepthSubscription(CSession& session, const std::string& security, bool subscribe)
{
	std::size_t dot = security.rfind('.');
	if (std::string::npos == dot)
	{
		return false;
	}
	CQuoteInfo quote(security.substr(0, dot), ParseMarket(security.substr(dot + 1)), Channel::depth);
	return session.SendRequest(subscribe ? request::Subscription(quote) : request::UnSubscription(quote));
}
