#pragma once
#include "ZhongTaiTypes.h"
#include "../../request/request.pb.h"
namespace trade::zhongtai
{
	class IZhongTaiStore
	{
	public:
		virtual ~IZhongTaiStore() = default;
		virtual CResult Load(const _TyAccount& account, request::ZhongTaiJournal& journal) = 0;
		virtual CResult Save(const _TyAccount& account, request::ZhongTaiJournal& journal) = 0;
		virtual void Release(const _TyAccount& account) = 0;
	};
	class CZhongTaiStore final : public IZhongTaiStore
	{
	public:
		explicit CZhongTaiStore(std::string owner);
		CResult Load(const _TyAccount& account, request::ZhongTaiJournal& journal) override;
		CResult Save(const _TyAccount& account, request::ZhongTaiJournal& journal) override;
		void Release(const _TyAccount& account) override;

	private:
		std::string m_owner;
		std::map<_TyAccount, std::map<std::uint64_t, std::string>> m_order_rows, m_trade_rows;
	};
} // namespace trade::zhongtai
