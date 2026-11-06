# 模拟柜台核心

本目录从 XEngine 迁入 XBroker。核心目标 `trading_counter` 不依赖数据库或网络；`CStorager` 使用 XBroker 自身复制的数据库层。`CTradeService::InitializeCounters` 创建柜台并注册工厂；行情及深度订阅请求通过 `CBrokerCenter` 转发，不依赖 XEngine 策略实现。

规则、费用、T+1、连续竞价撮合、共享深度流动性、账本事务及租约语义保持不变。无有效规则拒绝下单；行情恢复必须收到新深度。完整服务启动、测试和限制见项目根 README。
