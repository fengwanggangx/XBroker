#pragma once
#include "../counter/ITradeSession.h"
#include "../counter/CounterConfig.h"
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
namespace trade
{
	// Factories create login sessions. Each adapter owns its credentials and SDK session.
	class CCounterFactory final
	{
	public:
		using _TyStopHandler = std::function<void()>;
		using _TyFactory = std::function<std::shared_ptr<ITradeSession>(const CCounterConfig&)>;

		bool Register(const std::string& strKey, _TyFactory factory, _TyStopHandler stop = {})
		{
			if (strKey.empty() || nullptr == factory || !m_factory.emplace(strKey, std::move(factory)).second)
			{
				return false;
			}
			if (nullptr != stop)
			{
				try
				{
					m_stoppers.emplace_back(std::move(stop));
				}
				catch (...)
				{
					m_factory.erase(strKey);
					throw;
				}
			}
			return true;
		}

		bool Contains(const std::string& key) const
		{
			return m_factory.contains(key);
		}

		std::shared_ptr<ITradeSession> Create(const CCounterConfig& config) const
		{
			const auto mIter = m_factory.find(config.m_adapter);
			return m_factory.end() == mIter ? nullptr : mIter->second(config);
		}

		void Stop()
		{
			for (const auto& stop : m_stoppers)
			{
				stop();
			}
			m_stoppers.clear();
			m_factory.clear();
		}

	private:
		std::unordered_map<std::string, _TyFactory> m_factory;
		std::vector<_TyStopHandler> m_stoppers;
	};
} // namespace trade
