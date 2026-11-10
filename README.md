# XBroker

XBroker 是 C++20 柜台接入服务。账户身份由 `broker_id`、`site`、`account` 三个字段共同确定，账号按字符串精确匹配，保留前导零。不分配内部账户 ID，也不区分生产/模拟账户类型。

## 配置与登录

`ini/system.ini` 的 `[Trade] brokers_path` 指向券商目录 JSON，默认 `ini/brokers.json`：

```json
[{"id":1,"name":"模拟券商","adapter":"simulated","sites":["default"]}]
```

券商 ID 必须非零且唯一；名称用于展示，适配器键选择已注册的柜台实现，站点必须非空且不重复。未知适配器导致启动失败。真实券商通过 `CCounterFactory` 注册，当前不包含真实券商 SDK。

`SUPPORTED_BROKER_QUERY`（61）返回券商和站点目录。`TRADE_ACCOUNT_LOGIN`（8）通过 extra 传入 `broker_id/site/account/password`。登录时动态创建会话，成功后加入账户目录；并发重复登录返回冲突，已连接账户复用会话。失败账户不进入可用目录。密码仅在登录请求和适配器内部使用，不写入账户目录或数据库。

`TRADE_ACCOUNT_LIST_QUERY`（9）只列出本次运行已成功登录的账户。断线账户保留状态；Broker 重启后不自动登录，客户端重新提交凭据。后续资金、持仓、委托、成交及交易请求都携带三个身份字段，分别查询，不提供全量账户请求。


## 数据与部署

这是不兼容旧版本的协议更新。XBroker、XEngine、XTrader 必须配套升级，HQMarket 同步协议副本。使用新库 `broker_accounts_v2`，不读取旧账户配置表，不迁移或删除旧库。模拟适配器自行初始化并恢复账本、事务和写者租约。

服务认证使用 `[Broker] token`，默认 TCP 9803。MySQL、HQMarket 和行情规则配置见 `XBroker/exec/debug/ini/system.ini`。从配置目录所在的运行目录启动。只有模拟账本持久化；通用账户对象是运行期会话。

## 验证

正式服务由 `XBroker.sln` 中的 `XBroker/XBroker.vcxproj` 构建。CMake 文件保留为辅助验证配置。

静态线路协议一致性检查：

```sh
python tests/check_contracts.py
```

脚本比较 XBroker/XEngine 的请求、行情 `.proto` 及对应生成文件，共 6 个文件；不比较内部 ITradeSession、TradeTypes 或 request 帮助函数。失败时列出缺失或不一致的文件并返回非零退出码。该检查不代表业务语义或运行验证通过。

`tests/service_integration.py` 必须连接隔离 MySQL，以 `--binary --socket --mysql-port` 指定测试环境，可添加 `--xengine-binary` 验证终端协议到 XEngine/Broker 的链路。测试创建并清理自身进程命名的数据库和测试用户，使用合成行情源，不连接真实券商。

协议从 request.proto 使用 protoc 29.3 生成，并同步四个项目；不得手工修改生成文件。四端身份及请求枚举检查位于 `XTrader/tests/test_trade_account_protocol.py`。

服务不提供开户操作。模拟适配器仅登录已有持久化账本中的账户，未知账号登录失败；登录不创建账户或注入资金。

`tests/service_integration.py` 通过 `--ledger-fixture` 接收预置账本 SQL（包含表结构），使用券商1/default/000123、券商1/other/000123、券商2/default/000123、券商1/default/123，初始余额依次为10000000、20000000、30000000、40000000分。

交易适配器统一返回绑定单个账户的 `ITradeSession`：通过 `SetReplyHandler()` 一次注册响应入口，通过 `SendRequest(param)` 接收请求、`Snapshot()` 读取本账户快照、`SetEventHandler()` 接收事件。`Close()` 仅清理会话资源；业务退出先发送断开请求，成功后关闭会话。模拟账户存在活动委托时仍拒绝断开。

模拟实现由 `CSimulatedSession → CSimulatedDispatcher → CSimulatedCore` 组成；中泰实现由 `CZhongTaiSession → CZhongTaiDispatcher → IZhongTaiApi` 组成。每个账户一个 Session，同种适配器共享显式创建的 Dispatcher，不使用模拟柜台全局单例。Dispatcher 管理队列、工作线程、状态和持久化，服务停止时统一 `Stop()`。会话实例号隔离旧请求与回报。

请求入队成功后通过响应回调完成一次；入队失败不调用回调。事件可能先于请求响应。SDK 接受发送不代表交易所接受或成交，未知结果先查询核对，不自动重发。

深度行情按活动委托订阅：同一证券的首笔有效委托触发订阅，未收到有效盘口前等待撮合；全部委托成交、撤销或过期后退订。多个账户共享证券订阅，重连及账本恢复时按未完成委托重建订阅，发送失败会重试。交易规则不再作为预订阅清单。

中泰传统 XTP 2.2.50.8 原生接入见 [中泰适配器说明](XBroker/counter/zhongtai/README.md)。默认禁用，通过 `XBROKER_ENABLE_XTP` 和独立 SDK 路径启用；不自动连接或交易。

`CBootLoader` 创建并持有 TCP 服务、HQMarket Session、`CTradeService` 和 `CBrokerCenter`。中心只借用交易服务指针，统一接收终端请求、HQMarket 响应以及柜台响应和事件，负责认证、心跳、业务转发、响应回包和事件广播。交易服务不直接发送网络消息，通过中心绑定的回调提交响应、推送和行情订阅需求。

Dispatcher 由 `CTradeService` 以 `unique_ptr` 独占，交易服务负责初始化模拟和中泰柜台、注册工厂并管理账户 Session。HQMarket 行情与状态由中心转交交易服务，再传入模拟 Dispatcher。Dispatcher 各自校验配置并启动；未配置中泰路径时不注册中泰适配器。

退出由 BootLoader 统一协调：关闭 TCP 并排空终端请求，停止 HQMarket 并排空回调，清除网络处理器，停止交易服务（关闭账户 Session、停止柜台并等待回调、清理工厂与账户引用、销毁 Dispatcher），最后解绑并销毁中心、交易服务、网络端点和数据库。

交易服务通过 pending 表管理柜台请求，记录原连接、原请求 ID、账户、操作类型和截止时间；不保存密码或逐请求回调。每笔请求分配单调递增的内部 `m_correlationId`，柜台原样返回，原业务 `m_requestId/source_request_id` 保持不变。关联 ID 仅用于 XBroker 内部，不写入网络协议或持久化账本。响应统一经中心转交交易服务，按关联 ID 和账户、操作匹配并移除 pending；重复、迟到或不匹配的回包不再完成请求。pending 上限 4096，普通请求 30 秒、登录 120 秒到期；维护线程每 250 毫秒检查。报撤单超时返回结果未知，需查询核对，不自动重发。停止时先排空柜台回调，再失败完成剩余 pending。

`ITradeSession::GetId()` 返回对象生命周期内固定的进程内会话 ID，创建新 Session 时分配新 ID，关闭后保持不变。模拟和中泰 Session 在发布事件时设置 `CCountEvent::m_sessionId`。事件入口统一只传 `CCountEvent`，中心转交交易服务后，用事件账户查当前 Session，再核对 ID，丢弃旧会话或缺少 ID 的事件；不再额外绑定账户和 Session 弱引用。会话 ID 只在 XBroker 内使用，不写入网络协议及持久化账本。

## 正式程序构建入口

正式程序统一从 Visual Studio 解决方案构建，以 `.vcxproj` 为准。仓库中的 CMake 入口仅用于辅助验证，不作为正式程序构建、发布或验收依据。

打开 `XBroker.sln`，选择 `XBroker/XBroker.vcxproj`，使用 VS Linux 远程构建，配置为 Debug 或 Release、平台 x64；需在 VS 中配置 Linux 连接及工程所需依赖。
本次仅核对解决方案及工程文件，不执行编译、链接或运行测试。
