# 数据与兼容边界

物理表包括 meta、collections、versions、commits、receipts、index_defs、index_values、pins、consumers。versions 的主键为集合、键、修订；body 的 SQL NULL 表示删除，JSON null 只能作为文档字段。collections 保存当前模式；index_values 是当前 head 的投影。commits 保存完整的操作序列；receipts 保存不受历史回收影响的请求摘要与结果。幂等键不能被不同操作复用，回放不重新执行前置条件。

floor 是仍可读取的最早修订，不代表每条物理版本的最小修订；每个键在 floor 之前的最后版本可能是构造 floor 读视图的基准，必须保留。commits 仅需保存大于 floor 的提交，消费者从自己的 ack 之后读取。pin 与消费者位置不能低于现存 floor。

consumers 行保存名称、ack 与可选关注集合；关注集合为 NULL 表示订阅全部完整提交，因此旧库消费者在升级后仍按原语义读取。选择只在交付时投影，不改变 commits 中保存的完整操作序列，也不改变确认单调性。被完全过滤的提交不交付事件但仍计入 `skipped`，页的 `next` 覆盖已核对的连续区间，消费方据此推进 ack；后续页继续从 `next` 向后读取，不会跳过关注集合的提交。若消费者 ack 早于 floor（例如恢复后消费者行丢失、或释放后历史被回收），`pending`/`read` 明确拒绝，`reattach` 将位置提升到 floor 并报告丢弃的修订数。

模式包含 fields、required、additional，支持点路径类型；additional=false 限制顶层未知根字段，不递归禁止已声明对象中的子字段。字段不允许 null，除非该字段未声明类型；缺失可通过 required 控制。integer 排除 bool，number 接受整数及有限浮点。JSON 对象不允许非字符串键；浮点必须有限。索引忽略缺失字段与对象/数组，空值作为可索引标量，数值 1 和 1.0 共用索引值，true 与 1 不共用。

游标携带数据库身份、谓词摘要、读版本和最后键，并由数据库私有密钥签名。它不是权限凭据，不冻结模式，也不是保留租约。需要稳定的长时间读取，调用者自行管理具名保留点。

副本包是内部可信通道数据，校验和只用于检测传输损坏。正常备份恢复新主库保留逻辑内容而更换身份；副本初始化保留上游身份。分段追赶未完成时暂不提供当前索引，避免最新唯一约束错误约束中间历史视图。目录 epoch 可在没有文档提交时前进，最终空提交包仍能传递新的模式与索引。
