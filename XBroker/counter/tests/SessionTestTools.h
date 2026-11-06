#pragma once
#include "../ITradeSession.h"

namespace countertest
{
	template <class T>
	trade::CResult SendRequest(T& session, trade::CReqParam request, trade::ITradeSession::_TyReplyHandler handler)
	{
		session.SetReplyHandler(std::move(handler));
		return session.SendRequest(std::move(request));
	}
} // namespace countertest
