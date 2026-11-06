#include "system/CBootLoader.h"
#include <iostream>

int main()
{
	CBootLoader boot;
	if (!boot.Initialize() || !boot.Run())
	{
		std::cerr << boot.GetLastError() << '\n';
		return boot.GetErrorCode();
	}
	boot.Finalize();
	return 0;
}
