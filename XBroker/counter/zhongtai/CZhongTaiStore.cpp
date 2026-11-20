#include "CZhongTaiStore.h"
#include "../../database/CDBEngine.h"
#include "../../database/IDataBase.h"
namespace trade::zhongtai
{
	namespace
	{
		std::string Hex(const std::string& text)
		{
			static constexpr char digits[] = "0123456789abcdef";
			std::string result = "X'";
			result.reserve(text.size() * 2 + 3);
			for (unsigned char value : text)
			{
				result += digits[value >> 4];
				result += digits[value & 15];
			}
			return result + "'";
		}
		CResult Failure()
		{
			return { Error::Unavailable, "XTP journal unavailable or writer lease lost" };
		}
		class CTransaction
		{
		public:
			explicit CTransaction(db::IDataBase& database) : m_database(database)
			{
			}
			~CTransaction()
			{
				if (!m_committed)
				{
					m_database.RollBackTransaction();
				}
			}
			bool Commit()
			{
				m_committed = m_database.EndTransaction();
				return m_committed;
			}

		private:
			db::IDataBase& m_database;
			bool m_committed{ false };
		};
	} // namespace
	CZhongTaiStore::CZhongTaiStore(std::string owner) : m_owner(std::move(owner))
	{
	}
	CResult CZhongTaiStore::Load(const _TyAccount& account, request::ZhongTaiJournal& journal)
	{
		db::_TyDBPtr database = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr == database)
		{
			return Failure();
		}
		if (0 != database->ExecUpdate("CREATE TABLE IF NOT EXISTS xtp_journal(account_key VARBINARY(512) PRIMARY KEY,owner VARBINARY(128) NOT NULL,expires DATETIME(3) NOT NULL,revision BIGINT UNSIGNED NOT NULL,payload LONGBLOB NOT NULL) ENGINE=InnoDB"))
		{
			return Failure();
		}
		if (0 != database->ExecUpdate("CREATE TABLE IF NOT EXISTS xtp_events(account_key VARBINARY(512) NOT NULL,sequence BIGINT UNSIGNED NOT NULL,payload LONGBLOB NOT NULL,PRIMARY KEY(account_key,sequence)) ENGINE=InnoDB"))
		{
			return Failure();
		}
		for (const std::string& table : { std::string("xtp_orders"), std::string("xtp_trades") })
		{
			if (0 != database->ExecUpdate("CREATE TABLE IF NOT EXISTS " + table + "(account_key VARBINARY(512) NOT NULL,id BIGINT UNSIGNED NOT NULL,payload LONGBLOB NOT NULL,PRIMARY KEY(account_key,id)) ENGINE=InnoDB"))
			{
				return Failure();
			}
		}
		if (!database->BeginTransaction())
		{
			return Failure();
		}
		CTransaction transaction(*database);
		std::string key = Hex(EncodeAccount(account));
		if (0 != database->ExecUpdate("INSERT IGNORE INTO xtp_journal VALUES(" + key + "," + Hex(m_owner) + ",TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(3)),0,X'')"))
		{
			return Failure();
		}
		const db::CTable& row = database->ExecQuery("SELECT revision,payload,owner,IF(expires>CURRENT_TIMESTAMP(3),1,0) FROM xtp_journal WHERE account_key=" + key + " FOR UPDATE");
		if (1 != row.m_rows.size() || 4 != row.m_rows.front().size())
		{
			return Failure();
		}
		const db::_TyRow& values = row.m_rows.front();
		if (m_owner != db::to_string(values[2]) && "1" == db::to_string(values[3]))
		{
			return Failure();
		}
		std::string payload = db::to_string(values[1]);
		request::ZhongTaiJournal restored;
		if (!payload.empty() && !restored.ParseFromString(payload))
		{
			return Failure();
		}
		restored.set_revision(std::stoull(db::to_string(values[0])));
		if (0 != database->ExecUpdate("UPDATE xtp_journal SET owner=" + Hex(m_owner) + ",expires=TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(3)) WHERE account_key=" + key))
		{
			return Failure();
		}
		std::map<std::uint64_t, std::string> orders, trades;
		for (int kind = 0; kind < 2; ++kind)
		{
			const db::CTable& records = database->ExecQuery("SELECT id,payload FROM " + std::string(0 == kind ? "xtp_orders" : "xtp_trades") + " WHERE account_key=" + key);
			if (records.m_columns.empty())
			{
				return Failure();
			}
			for (const auto& record : records.m_rows)
			{
				if (2 != record.size())
				{
					return Failure();
				}
				std::uint64_t id = std::stoull(db::to_string(record[0]));
				std::string body = db::to_string(record[1]);
				if (0 == kind)
				{
					if (!restored.add_orders()->ParseFromString(body))
					{
						return Failure();
					}
					orders.emplace(id, std::move(body));
				}
				else
				{
					if (!restored.add_trades()->ParseFromString(body))
					{
						return Failure();
					}
					trades.emplace(id, std::move(body));
				}
			}
		}
		if (!transaction.Commit())
		{
			return Failure();
		}
		m_order_rows[account] = std::move(orders);
		m_trade_rows[account] = std::move(trades);
		journal = std::move(restored);
		return {};
	}
	CResult CZhongTaiStore::Save(const _TyAccount& account, request::ZhongTaiJournal& journal)
	{
		db::_TyDBPtr database = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr == database || !database->BeginTransaction())
		{
			return Failure();
		}
		CTransaction transaction(*database);
		std::string key = Hex(EncodeAccount(account));
		const db::CTable& row = database->ExecQuery("SELECT revision,owner,IF(expires>CURRENT_TIMESTAMP(3),1,0) FROM xtp_journal WHERE account_key=" + key + " FOR UPDATE");
		if (1 != row.m_rows.size() || 3 != row.m_rows.front().size())
		{
			return Failure();
		}
		const db::_TyRow& values = row.m_rows.front();
		if (std::to_string(journal.revision()) != db::to_string(values[0]) || m_owner != db::to_string(values[1]) || "1" != db::to_string(values[2]))
		{
			return Failure();
		}
		std::map<std::uint64_t, std::string> changedOrders, changedTrades;
		for (const auto& order : journal.orders())
		{
			std::string body = order.SerializeAsString();
			const auto mIter = m_order_rows[account].find(order.order_id());
			if (m_order_rows[account].end() == mIter || body != mIter->second)
			{
				if (0 != database->ExecUpdate("INSERT INTO xtp_orders VALUES(" + key + "," + std::to_string(order.order_id()) + "," + Hex(body) + ") ON DUPLICATE KEY UPDATE payload=VALUES(payload)"))
				{
					return Failure();
				}
				changedOrders.emplace(order.order_id(), std::move(body));
			}
		}
		for (const auto& trade : journal.trades())
		{
			std::string body = trade.SerializeAsString();
			const auto mIter = m_trade_rows[account].find(trade.trade_id());
			if (m_trade_rows[account].end() == mIter || body != mIter->second)
			{
				if (0 != database->ExecUpdate("INSERT INTO xtp_trades VALUES(" + key + "," + std::to_string(trade.trade_id()) + "," + Hex(body) + ") ON DUPLICATE KEY UPDATE payload=VALUES(payload)"))
				{
					return Failure();
				}
				changedTrades.emplace(trade.trade_id(), std::move(body));
			}
		}
		request::ZhongTaiJournal next = journal;
		next.set_revision(journal.revision() + 1);
		for (const auto& event : journal.events())
		{
			if (0 != database->ExecUpdate("INSERT INTO xtp_events VALUES(" + key + "," + std::to_string(event.sequence()) + "," + Hex(event.SerializeAsString()) + ")"))
			{
				return Failure();
			}
		}
		next.clear_events();
		next.clear_orders();
		next.clear_trades();
		if (0 != database->ExecUpdate("UPDATE xtp_journal SET revision=" + std::to_string(next.revision()) + ",payload=" + Hex(next.SerializeAsString()) + ",expires=TIMESTAMPADD(SECOND,30,CURRENT_TIMESTAMP(3)) WHERE account_key=" + key) || !transaction.Commit())
		{
			return Failure();
		}
		for (auto& item : changedOrders)
		{
			m_order_rows[account].insert_or_assign(item.first, std::move(item.second));
		}
		for (auto& item : changedTrades)
		{
			m_trade_rows[account].insert_or_assign(item.first, std::move(item.second));
		}
		journal.set_revision(next.revision());
		journal.clear_events();
		return {};
	}
	void CZhongTaiStore::Release(const _TyAccount& account)
	{
		db::_TyDBPtr database = CDBEngine::InstanceRef().GetDBPtr(db::em_database::mysql);
		if (nullptr != database)
		{
			database->ExecUpdate("UPDATE xtp_journal SET expires=CURRENT_TIMESTAMP(3) WHERE account_key=" + Hex(EncodeAccount(account)) + " AND owner=" + Hex(m_owner));
		}
	}
} // namespace trade::zhongtai
