#pragma once
#include "TradeTypes.h"

namespace trade
{
	// One command owns an undo journal. Only touched records are copied; history is append-only.
	class CStateEdit final
	{
		public:
		explicit CStateEdit(CState& state) : m_state(state)
		{
			m_original.m_next_order = state.m_next_order;
			m_original.m_next_trade = state.m_next_trade;
			m_original.m_sequence = state.m_sequence;
			m_original.m_settled_day = state.m_settled_day;
			m_original.m_closed_day = state.m_closed_day;
			m_original_event_count = state.m_events.size();
		}
		CStateEdit(const CStateEdit&) = delete;
		CStateEdit& operator=(const CStateEdit&) = delete;
		~CStateEdit()
		{
			Rollback();
		}
		CState& State()
		{
			return m_state;
		}
		std::uint64_t Sequence() const
		{
			return m_original.m_sequence;
		}
		void Accept()
		{
			m_finished = true;
		}
		template <class Key, class Value>
		void Save(std::map<Key, Value> CState::* member, const Key& key)
		{
			auto& touched = m_touched.*member;
			if (touched.contains(key))
			{
				return;
			}
			auto existing = (m_state.*member).find(key);
			if ((m_state.*member).end() != existing)
			{
				(m_original.*member).emplace(*existing);
			}
			touched.try_emplace(key);
		}
		CState Changes() const
		{
			CState changes;
			changes.m_next_order = m_state.m_next_order;
			changes.m_next_trade = m_state.m_next_trade;
			changes.m_sequence = m_state.m_sequence;
			changes.m_settled_day = m_state.m_settled_day;
			changes.m_closed_day = m_state.m_closed_day;
			Copy(&CState::m_accounts, changes);
			Copy(&CState::m_positions, changes);
			Copy(&CState::m_orders, changes);
			Copy(&CState::m_trades, changes);
			Copy(&CState::m_rules, changes);
			Copy(&CState::m_calendar, changes);
			Copy(&CState::m_consumed_books, changes);
			changes.m_events.assign(m_state.m_events.begin() + m_original_event_count, m_state.m_events.end());
			return changes;
		}
		void Rollback()
		{
			if (m_finished)
			{
				return;
			}
			Restore(&CState::m_accounts);
			Restore(&CState::m_positions);
			Restore(&CState::m_orders);
			Restore(&CState::m_trades);
			Restore(&CState::m_rules);
			Restore(&CState::m_calendar);
			Restore(&CState::m_consumed_books);
			m_state.m_next_order = m_original.m_next_order;
			m_state.m_next_trade = m_original.m_next_trade;
			m_state.m_sequence = m_original.m_sequence;
			m_state.m_settled_day = m_original.m_settled_day;
			m_state.m_closed_day = m_original.m_closed_day;
			m_state.m_events.resize(m_original_event_count);
			m_finished = true;
		}

		private:
		template <class Key, class Value>
		void Copy(std::map<Key, Value> CState::* member, CState& changes) const
		{
			for (const auto& item : m_touched.*member)
			{
				(changes.*member).emplace(item.first, (m_state.*member).at(item.first));
			}
		}
		template <class Key, class Value>
		void Restore(std::map<Key, Value> CState::* member)
		{
			for (const auto& item : m_touched.*member)
			{
				auto original = (m_original.*member).find(item.first);
				if ((m_original.*member).end() == original)
				{
					(m_state.*member).erase(item.first);
				}
				else
				{
					std::swap((m_state.*member).at(item.first), original->second);
				}
			}
		}
		CState& m_state;
		CState m_original, m_touched;
		std::size_t m_original_event_count{ 0 };
		bool m_finished{ false };
	};
} // namespace trade
