#pragma once
#include "../request/request.h"
#include "../request/request.pb.h"
#include "../service/AccountInfo.h"
#include "TradeTypes.h"
namespace trade::protocol
{
	request::TradeAccountRecord AccountPayload(const CTradeAccountInfo& info);
	void WriteOrder(const COrder& row, request::TradeOrderRecord& wire);
	void WriteTrade(const CTrade& row, request::TradeRecord& wire);
	void WritePosition(const CPosition& row, request::TradePositionRecord& wire);
	void WriteFunds(const CFunds& row, bool bReady, request::TradeAccountInfo& wire);
	void WriteReply(const CCounterReply& reply, ::CRequest& response);
	void WriteEvent(const CCountEvent& event, request::TradeEventRecord& wire);
	::CRequest Push(const CCountEvent& event, const CTradeAccountInfo& account);
} // namespace trade::protocol
