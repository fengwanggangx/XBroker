#include "log/CLogger.h"
#include "system/CBootLoader.h"
#include <iostream>

int main()
{
	CLogger::Initialize("xbroker");
	CLogger::Write(Level::Info, "startup", "Service starting");
	CBootLoader boot;
	if (!boot.Initialize() || !boot.Run())
	{
		CLogger::Write(Level::Error, "startup", boot.GetLastError());
		return boot.GetErrorCode();
	}
	boot.Finalize();
	CLogger::Write(Level::Info, "startup", "Service stopped");
	return 0;
}
