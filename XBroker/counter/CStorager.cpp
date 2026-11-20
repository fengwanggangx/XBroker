#include "CStorager.h"
#include <charconv>
#include "../database/CDBEngine.h"
namespace trade
{
	namespace
	{
		bool Execute(db::IDataBase& database, const std::string& strSql)
		{
			return 0 == database.ExecUpdate(strSql);
		}
		bool Query(db::IDataBase& database, const std::string& strSql, std::vector<std::vector<std::string>>& rows)
		{
			const db::CTable& table = database.ExecQuery(strSql);
			if (table.m_columns.empty())
			{
				return false;
			}
			rows.clear();
			rows.reserve(table.m_rows.size());
			for (const auto& row : table.m_rows)
			{
				std::vector<std::string> values;
				values.reserve(row.size());
				for (const auto& value : row)
				{
					values.emplace_back(db::to_string(value));
				}
				rows.emplace_back(std::move(values));
			}
			return true;
		}
		std::string Hex(const std::string& strValue)
		{
			static constexpr char Digits[] = "0123456789abcdef";
			std::string strHex;
			strHex.reserve(strValue.size() * 2 + 3);
			strHex = "X'";
			for (unsigned char nValue : strValue)
			{
				strHex += Digits[nValue >> 4];
				strHex += Digits[nValue & 15];
			}
			return strHex + "'";
		}
		bool Number(const std::string& strText, std::uint64_t& nValue)
		{
			std::from_chars_result result = std::from_chars(strText.data(), strText.data() + strText.size(), nValue);
			return (std::errc() == result.ec) && (strText.data() + strText.size() == result.ptr);
		}
		class CTransaction
		{
		public:
			explicit CTransaction(db::IDataBase& session) : m_session(session)
			{
			}
			~CTransaction()
			{
				if (!m_committed)
				{
					m_session.RollBackTransaction();
				}
			}
			bool Commit()
			{
				m_committed = m_session.EndTransaction();
				return m_committed;
			}

		private:
			db::IDataBase& m_session;
			bool m_committed{ false };
		};
		CResult Failure()
		{
			return { Error::Unavailable, "counter storage or lease unavailable; resynchronize before retry" };
		}
	} // namespace
	CStorager::CStorager(std::string strOwner) : m_owner(std::move(strOwner))
	{
	}
	bool CStorager::CheckFence(db::IDataBase& session, bool bAcquire)
	{
		std::vector<std::vector<std::string>> rows;
		if (!Query(session, "SELECT owner,fence,IF(expires_at>CURRENT_TIMESTAMP(3),1,0) FROM sim_counter_lease WHERE id=1 FOR UPDATE", rows) || (1 != rows.size()) || (3 != rows.front().size()))
		{
			return false;
		}
		std::uint64_t nFence = 0;
		if (!Number(rows.front()[1], nFence))
		{
			return false;
		}
		if (bAcquire)
		{
			if ("1" == rows.front()[2])
			{
				return false;
			}
			if (!Execute(session, "UPDATE sim_counter_lease SET owner=" + Hex(m_owner) + ",fence=fence+1,expires_at=TIMESTAMPADD(SECOND,15,CURRENT_TIMESTAMP(3)) WHERE id=1"))
			{
				return false;
			}
			m_fence = nFence + 1;
			return true;
		}
		if ((rows.front()[0] != m_owner) || (m_fence != nFence) || ("1" != rows.front()[2]))
		{
			return false;
		}
		return Execute(session, "UPDATE sim_counter_lease SET expires_at=TIMESTAMPADD(SECOND,15,CURRENT_TIMESTAMP(3)) WHERE id=1");
	}
	CResult CStorager::Acquire(CState& state)
	{
		db::_TyDBPtr session = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr == session)
		{
			return Failure();
		}
		for (const auto& strSql : {
				 "CREATE TABLE IF NOT EXISTS sim_counter_lease(id INT PRIMARY KEY,owner VARBINARY(128) NOT NULL,fence BIGINT UNSIGNED NOT NULL,expires_at DATETIME(3) NOT NULL) ENGINE=InnoDB",
				 "INSERT IGNORE INTO sim_counter_lease VALUES(1,'',0,'1970-01-01 00:00:01')",
				 "CREATE TABLE IF NOT EXISTS sim_counter_checkpoint(id INT PRIMARY KEY,sequence BIGINT UNSIGNED NOT NULL,body LONGBLOB NOT NULL) ENGINE=InnoDB" })
		{
			if (!Execute(*session, strSql))
			{
				return Failure();
			}
		}
		for (const auto& strTable : { "accounts", "positions", "orders", "trades", "rules", "calendar", "events", "books" })
		{
			if (!Execute(*session, "CREATE TABLE IF NOT EXISTS sim_counter_" + std::string(strTable) + "(record_key VARBINARY(512) PRIMARY KEY,body LONGBLOB NOT NULL) ENGINE=InnoDB"))
			{
				return Failure();
			}
		}
		if (!session->BeginTransaction())
		{
			return Failure();
		}
		CTransaction transaction(*session);
		if (!CheckFence(*session, true))
		{
			return Failure();
		}
		std::vector<std::vector<std::string>> rows;
		if (!Query(*session, "SELECT body FROM sim_counter_checkpoint WHERE id=1", rows))
		{
			return Failure();
		}
		CState restored;
		if (!rows.empty())
		{
			if (1 != rows.front().size() || !rows.front()[0].starts_with("ROWS1\n") || !DecodeState(rows.front()[0].substr(6), restored))
			{
				return { Error::Rejected, "corrupted counter metadata" };
			}
			// Metadata carries counters only. Transactional entity rows are the ledger.
			auto load = [&session, &rows, &restored](const std::string& strTable, auto member)
			{
				if (!Query(*session, "SELECT body FROM sim_counter_" + strTable, rows))
				{
					return false;
				}
				for (const auto& row : rows)
				{
					CState record;
					if (1 != row.size() || !DecodeState(row[0], record) || 1 != (record.*member).size())
					{
						return false;
					}
					if (!(restored.*member).emplace(*(record.*member).begin()).second)
					{
						return false;
					}
				}
				return true;
			};
			if (!load("accounts", &CState::m_accounts) || !load("positions", &CState::m_positions) || !load("orders", &CState::m_orders) || !load("trades", &CState::m_trades) || !load("rules", &CState::m_rules) || !load("calendar", &CState::m_calendar) || !load("books", &CState::m_consumed_books))
			{
				return { Error::Rejected, "counter row restore failed" };
			}
			if (!Query(*session, "SELECT body FROM sim_counter_events", rows))
			{
				return Failure();
			}
			std::map<std::uint64_t, CCountEvent> events;
			for (const auto& row : rows)
			{
				CState record;
				if (1 != row.size() || !DecodeState(row[0], record) || 1 != record.m_events.size() || !events.emplace(record.m_events[0].m_sequence, record.m_events[0]).second)
				{
					return { Error::Rejected, "corrupted event row" };
				}
			}
			for (auto& item : events)
			{
				restored.m_events.emplace_back(std::move(item.second));
			}
		}
		if (!transaction.Commit())
		{
			return Failure();
		}
		state = std::move(restored);
		return {};
	}
	CResult CStorager::Commit(const CState& state, std::uint64_t nExpectedSequence)
	{
		db::_TyDBPtr session = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if ((nullptr == session) || !session->BeginTransaction())
		{
			return Failure();
		}
		CTransaction transaction(*session);
		if (!CheckFence(*session, false))
		{
			return Failure();
		}
		std::vector<std::vector<std::string>> rows;
		if (!Query(*session, "SELECT sequence FROM sim_counter_checkpoint WHERE id=1 FOR UPDATE", rows))
		{
			return Failure();
		}
		std::uint64_t nSequence = 0;
		if (!rows.empty() && ((1 != rows.front().size()) || !Number(rows.front()[0], nSequence)))
		{
			return Failure();
		}
		if (nSequence != nExpectedSequence)
		{
			return { Error::Conflict, "checkpoint version changed" };
		}
		auto upsert = [&session](const std::string& strTable, const std::string& strKey, const CState& row)
		{
			return Execute(*session, "INSERT INTO sim_counter_" + strTable + "(record_key,body) VALUES(" + Hex(strKey) + "," + Hex(EncodeState(row)) + ") ON DUPLICATE KEY UPDATE body=VALUES(body)");
		};
		// The caller supplies only touched records; unrelated history is never rewritten.
		for (const auto& item : state.m_accounts)
		{
			CState row;
			row.m_accounts.emplace(item);
			if (!upsert("accounts", EncodeAccount(item.first), row))
			{
				return Failure();
			}
		}
		for (const auto& item : state.m_positions)
		{
			CState row;
			row.m_positions.emplace(item);
			if (!upsert("positions", EncodeAccount(item.first.first) + ":" + item.first.second, row))
			{
				return Failure();
			}
		}
		for (const auto& item : state.m_orders)
		{
			CState row;
			row.m_orders.emplace(item);
			if (!upsert("orders", std::to_string(item.first), row))
			{
				return Failure();
			}
		}
		for (const auto& item : state.m_trades)
		{
			CState row;
			row.m_trades.emplace(item);
			if (!upsert("trades", std::to_string(item.first), row))
			{
				return Failure();
			}
		}
		for (const auto& item : state.m_rules)
		{
			CState row;
			row.m_rules.emplace(item);
			if (!upsert("rules", std::to_string(item.first.first) + ":" + item.first.second, row))
			{
				return Failure();
			}
		}
		for (const auto& item : state.m_calendar)
		{
			CState row;
			row.m_calendar.emplace(item);
			if (!upsert("calendar", std::to_string(item.first), row))
			{
				return Failure();
			}
		}
		for (const auto& item : state.m_consumed_books)
		{
			CState row;
			row.m_consumed_books.emplace(item);
			if (!upsert("books", item.first, row))
			{
				return Failure();
			}
		}
		for (const auto& event : state.m_events)
		{
			if (event.m_sequence <= nExpectedSequence)
			{
				continue;
			}
			CState row;
			row.m_events.emplace_back(event);
			if (!Execute(*session, "INSERT INTO sim_counter_events(record_key,body) VALUES(" + Hex(std::to_string(event.m_sequence)) + "," + Hex(EncodeState(row)) + ")"))
			{
				return Failure();
			}
		}
		CState metadata;
		metadata.m_next_order = state.m_next_order;
		metadata.m_next_trade = state.m_next_trade;
		metadata.m_sequence = state.m_sequence;
		metadata.m_settled_day = state.m_settled_day;
		metadata.m_closed_day = state.m_closed_day;
		if (!Execute(*session, "INSERT INTO sim_counter_checkpoint(id,sequence,body) VALUES(1," + std::to_string(state.m_sequence) + "," + Hex("ROWS1\n" + EncodeState(metadata)) + ") ON DUPLICATE KEY UPDATE sequence=VALUES(sequence),body=VALUES(body)"))
		{
			return Failure();
		}
		// A transaction that outlived the lease cannot publish a new checkpoint.
		if (!CheckFence(*session, false))
		{
			return Failure();
		}
		if (!transaction.Commit())
		{
			return { Error::UnknownResult, "commit outcome unknown; reload checkpoint" };
		}
		return {};
	}
	CResult CStorager::Renew()
	{
		db::_TyDBPtr session = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if ((nullptr == session) || !session->BeginTransaction())
		{
			return Failure();
		}
		CTransaction transaction(*session);
		if (!CheckFence(*session, false) || !transaction.Commit())
		{
			return Failure();
		}
		return {};
	}
	void CStorager::Release()
	{
		if (0 == m_fence)
		{
			return;
		}
		db::_TyDBPtr session = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr != session)
		{
			Execute(*session, "UPDATE sim_counter_lease SET expires_at='1970-01-01 00:00:01' WHERE id=1 AND owner=" + Hex(m_owner) + " AND fence=" + std::to_string(m_fence));
		}
		m_fence = 0;
	}
} // namespace trade
