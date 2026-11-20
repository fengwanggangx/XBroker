#pragma once
#include "TradeTypes.h"
#include <string>

namespace trade
{
	struct CCounterConfig
	{
		_TyAccount m_account_index{};
		std::string m_adapter;
	};
} // namespace trade
