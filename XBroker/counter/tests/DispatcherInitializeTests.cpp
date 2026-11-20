#include "../CSimulatedDispatcher.h"
#include "../zhongtai/CZhongTaiDispatcher.h"
#include "../../ini/CINIHandler.h"
#include <iostream>
#include <stdexcept>

namespace
{
	void Check(bool condition, const char* message)
	{
		if (!condition)
		{
			throw std::runtime_error(message);
		}
	}

	void Run()
	{
		ini::CINIHandler& config = ini::CINIHandler::InstanceRef();
		Check(config.SetValue(ini::Config::System, "Trade", "zhongtai_path", std::string()), "clear ZhongTai configuration");
		CZhongTaiDispatcher zhongTaiDispatcher;
		Check(zhongTaiDispatcher.Initialize().Ok(), "unconfigured ZhongTai remains optional");
		Check(!zhongTaiDispatcher.IsInitialized(), "optional ZhongTai must not start a worker");
		zhongTaiDispatcher.Stop();
		zhongTaiDispatcher.Stop();
		Check(config.SetValue(ini::Config::System, "Trade", "zhongtai_path", std::string("missing-zhongtai.json")), "set ZhongTai configuration");
		trade::CResult result = zhongTaiDispatcher.Initialize();
		Check(!result.Ok() && !zhongTaiDispatcher.IsInitialized(), "configured ZhongTai failure leaves no worker");
		Check(config.SetValue(ini::Config::System, "Trade", "zhongtai_path", std::string()), "disable ZhongTai after failure");
		Check(zhongTaiDispatcher.Initialize().Ok() && !zhongTaiDispatcher.IsInitialized(), "ZhongTai can retry after initialization failure");

		CSimulatedDispatcher simulatedDispatcher;
		Check(config.SetValue(ini::Config::System, "Trade", "rules_path", std::string("missing-rules.json")), "set missing rules");
		result = simulatedDispatcher.Initialize();
		Check(trade::Error::Rejected == result.m_error && "cannot open Trade.rules_path" == result.m_message, "simulated initialization reads its rules path");
		Check(config.SetValue(ini::Config::System, "Trade", "rules_path", std::string("invalid-rules.json")), "set invalid rules");
		result = simulatedDispatcher.Initialize();
		Check(trade::Error::Rejected == result.m_error && "invalid counter rules JSON" == result.m_message, "simulated retry validates the rules document");
		Check(config.SetValue(ini::Config::System, "Trade", "rules_path", std::string("invalid-calendar.json")), "set invalid calendar");
		result = simulatedDispatcher.Initialize();
		Check(trade::Error::Rejected == result.m_error && "calendar requires explicit day and epoch-millisecond sessions" == result.m_message, "simulated initialization validates calendar rows");
		simulatedDispatcher.Stop();
		simulatedDispatcher.Stop();
	}
} // namespace

int main()
{
	try
	{
		Run();
		std::cout << "Dispatcher initialization tests passed\n";
		return 0;
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
}
