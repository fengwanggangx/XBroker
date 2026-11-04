#include "../TradeProtocol.h"
#include <iostream>
int main()
{
	_TyAccount account{ 1, "000123" };
	CTradeAccountInfo info = AccountInfo(account);
	info.m_strBrokerName = "鍒稿晢";
	if (account != AccountIndex(trade::protocol::AccountPayload(info)))
	{
		return 1;
	}
	trade::CCounterReply reply;
	reply.m_accountIndex = account;
	reply.m_data = trade::CFunds{ account, 10000, 0, true, true };
	CRequest wire;
	trade::protocol::WriteReply(reply, wire);
	if (account != AccountIndex(wire.GetData().counter_reply().funds()))
	{
		return 2;
	}
	std::cout << "broker identity protocol passed\n";
}
