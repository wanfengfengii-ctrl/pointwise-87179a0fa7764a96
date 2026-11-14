# Avro OCF 入湖质量验证报告

- 固定种子：`20261114`（见 evidence.json 顶层 `seed`，各矩阵以 `矩阵名:种子` 派生随机源，可重跑）
- 验证入口：`python3 qa.py verify` / `python3 qa.py demo`
- 原始回归：`python3 checks.py` 仍输出 `AVRO_NORMAL_OK`，原断言未改动
- 结论摘要：物理取帧、损坏拒绝、失败后实例隔离与缓存计数均符合契约；发现 1 项生产偏差 **COR-B1-ARRAY-SEG（阻断）**，失败输入已保存，未修复业务代码。

## 1. 源码边界

本轮只新增验证资产，生产源码 `schemas.py`、`codec.py`、`resolver.py`、`importer.py`、`fixture.py`、`samples.py`、`checks.py` 全部保持原样（与初始提交一致，无生产功能修复）。

新增文件：

| 文件 | 角色 | 依赖边界 |
| --- | --- | --- |
| `frame_oracle.py` | 独立字节取帧器，只解析外框 | 不导入 schemas/codec/resolver/importer/fixture，也不使用任何其他 Avro 读取器；读取逻辑自包含 |
| `qa.py` | 质量驱动：回归、互核、矩阵、缓存、变异、压力、报告核对 | 可调用生产入口与 fixture；本地构造器仅用于造数，取帧器不调用它 |
| `evidence.json` | verify 每次重新生成的证据 | 含案例 ID、重建参数、种子、字节补丁与原始偏移、帧摘要/错误位置、期望/实际、逐调用缓存计数、变异见证 |
| `qa_artifacts/fail_inputs/` | 偏差与变异输入留存 | 阻断输入 `COR-B1-ARRAY-SEG.bin` 可直接重跑 |

变异只在 `qa_artifacts/mutations/` 下的隔离副本中执行，工作区生产文件不被打补丁。

## 2. 取帧器的判定范围

`frame_oracle.inspect(blob)` 独立检查：魔数、元数据 map（正/负计数段、段消费、重复键、avro.schema 的 UTF-8 JSON、avro.codec=null）、头 sync、每个记录块的正记录数、非负载荷长度、恰长载荷区间和块 sync。输出 `header_end/sync/blocks`，所有偏移左闭右开并通过 JSON 可序列化校验（ORC-EMPTY、ORC-NULL-PAYLOAD、ORC-NEG-META、ORC-EMBEDDED-SYNC）。

判定边界（契约第 35 条）：

1. 记录载荷对取帧器是不透明字节，不解析模式图或记录值，也不判定正记录数与实际载荷是否匹配。COR-B1-ARRAY-SEG 的取帧结果为外框合法（`sync_ok=true`），业务拒绝与否由测试驱动依据已知构造另行判定，本报告不声称取帧器验证了记录语义。
2. 外框非法时抛 `OracleError`，带 `code/offset`。单故障分类与偏移在 ORC-ERR-* 共 16 个手工微型案例中逐一核对：magic（ORC-ERR-MAGIC）、varint 截断/10 字节续位/溢出（ORC-ERR-VARINTTR、ORC-ERR-VARINT10、ORC-ERR-VARINTOF）、负长度（ORC-ERR-LEN、ORC-ERR-BLOCKLEN）、metadata 段消费/重复键/缺 schema/坏 JSON（ORC-ERR-META-*）、非正块计数（ORC-ERR-COUNT）、sync（ORC-ERR-SYNC）、截断首个缺失字节即 `len(blob)`（ORC-ERR-ZERO、ORC-ERR-SHORT、ORC-ERR-HDRSYNC、ORC-ERR-BLOCKTR）。
3. 微型容器的偏移采用手工可核对的字面布局交叉核验，不与生产解析器互相背书：例如 ORC-NULL-PAYLOAD 中两块零字节载荷的 `start/payload_start/payload_end/end` 由 `header_end` 与定长字段（count 1 字节、长度 1 字节、sync 16 字节）逐步推算并断言；造数辅助编码逻辑位于 qa.py，取帧器无任何导入关系。
4. ORC-EMBEDDED-SYNC 证明长度框定优先：与 sync 完全相同的字节出现在载荷内部时取帧与生产导入均成功，未使用 sync 搜索代替取帧。

## 3. 覆盖依据

### 3.1 合法矩阵（LEG-*，逐行核对期望与实际）

- 空容器（LEG-EMPTY，含完整头、blocks=[]，生产返回 `[]`）；
- 单块（LEG-SINGLE）、多块与不同 block_rows（LEG-MULTI-1、LEG-MULTI-2、LEG-BLOCK32，40 行跨 32 行分块）；
- 重复行保持（LEG-DUP）；
- 数组/联合混合载荷（LEG-RICH，25 行，union 三分支与 0–4 长度数组，固定种子生成）；
- 负计数元数据段的合法容器（LEG-NEGMETA）；
- 合法前缀：在完整头后及每个完整块末尾截短（LEG-PREFIX-0 至 LEG-PREFIX-3），取帧器与生产导入都返回对应前缀行，不误报损坏；
- 正记录数零字节载荷：COR-NULL-PAYLOAD-LEGAL 与 ORC-NULL-PAYLOAD 证明全 null 记录的零长度块被合法接受。

### 3.2 损坏矩阵（COR-*，期望均为 codec.DecodeError 且不返回前缀）

- 首部：魔数（COR-HDR-MAGIC）、变长整数截断与 10 字节溢出（COR-HDR-VARINT-TRUNC、COR-HDR-VARINT-OVERFLOW）、负长度（COR-HDR-NEG-LENGTH）、元数据负段消费不符（COR-HDR-META-SEG）、重复键（COR-HDR-META-DUP）、schema JSON 损坏（COR-HDR-META-JSON）、头 sync 不符（COR-HDR-SYNC）；
- 首块：非正计数（COR-B1-COUNT）、负载荷长度（COR-B1-NEG-SIZE）、声明长度与实际不符（COR-B1-SIZE-MISMATCH）、声明记录数不符（COR-B1-COUNT-MISMATCH）、块 sync 不符（COR-B1-SYNC）、载荷内与块 sync 前截断（COR-B1-TRUNC-PAYLOAD、COR-B1-TRUNC-SYNC）、字符串非法 UTF-8（COR-B1-UTF8）、联合索引越界（COR-B1-UNION）、int 32 位越界（COR-B1-INT-RANGE）、数组负段（COR-B1-ARRAY-SEG，见第 6 节阻断）；
- 合法前缀之后失败：第二块非正计数（COR-B2-COUNT）、第二块 sync 不符（COR-B2-SYNC）、第二块截断（COR-B2-TRUNC）。

除阻断项外，22 个损坏案例均以 `DecodeError` 拒绝，证据中保留实际错误消息（消息文本不固定，只断言异常类别）。所有损坏均由确定性字节变换构造，补丁偏移与取值记录在 evidence.json 的 `patch` 字段，未变换输入补丁为空列表。

## 4. 失败隔离结论（CACHE-*）

同一 `Importer` 实例上逐调用记录 `hits/misses` 前后值，核对计划计数在“头解析”和“记录读取”分界处累计：

- 头失败不查计划：CACHE-HDR-FAIL 后计数保持 (0,0)；
- 头成功后记录前恰好一次计划查找：CACHE-B1-FAIL 首块失败后为 (0,1)，失败不撤销计划；
- 失败后再导入合法文件等于冷启动：CACHE-AFTER-FAIL 的行与新建 Importer 的结果逐行相等且命中 (1,1)；块尾 sync 失败后同样成立（CACHE-TAIL-FAIL、CACHE-AFTER-TAIL）；
- 空容器也进行一次查找（CACHE-EMPTY）；
- 缓存键按规范内容：字段字典键次序不同不产生新计划（CACHE-KEY-ORDER，仅 hits+1）；
- 交换文件次序不串行（CACHE-SWAP）；失败后不存在可被下次调用返回的旧行（CACHE-NO-STALE、CACHE-NO-STALE-CHECK）；
- `clear_cache()` 清空计划并把两个计数归零，再次合法导入重新 miss（CACHE-CLEARED-MISS）。

验收只使用 `hits/misses` 公开计数，未依赖私有 `_plans`。

## 5. 变异检出（MUT-*，隔离副本实际运行）

两类变异均在 `qa_artifacts/mutations/` 的副本中实际执行，输入同时送入原始生产代码（pristine）与变异体（mutant），见证完整保留在 evidence.json：

1. MUT-SKIP-SYNC：删除 `codec.records` 的块 sync 校验。对末块 sync 翻转 0xC3 的输入，原始代码返回 `DecodeError: sync`，变异体却成功返回全部 8 行业务行——证明“sync 必须一致”的业务判据确实在拦截损坏批次。
2. MUT-SWALLOW-TAIL：在 `importer.import_file` 吞掉块尾 `DecodeError` 并返回已解前缀（配套补 `DecodeError` 导入，仅存在于副本）。对块内截断输入，原始代码 `DecodeError: truncated` 且无返回，变异体以 `rows` 结局成功返回——证明“失败不得返回已解析前缀”的判据有效。

两次都是变异体产生了成功返回的错误业务结果才计为检出；导入失败、语法错误或普通异常退出不计为检出（runner 异常会被 qa 判为 QA_FAILED）。

## 6. 阻断偏差：COR-B1-ARRAY-SEG

- 现象：数组负计数段声明的段字节数被改大（单块容器，偏移 167，原值 2 改 4，补丁与布局断言见 evidence.json），外框仍然合法（取帧器 `sync_ok=true`，长度框定未被破坏），但生产代码未拒绝，静默返回错误行 `[{"t": [2, 2]}]`，期望为 `DecodeError`。
- 根因定位（未修改）：`codec.py` 数组负段分支（`codec.py:67-73`）只用 `block.read(1)` 检查段内是否有剩余字节（消费不足），当解析越过声明段尾（消费过多）时没有边界校验，后续字节被当成数组元素继续解码。元数据负段有同等消费检查且工作正常（COR-HDR-META-SEG 正确拒绝），数组路径缺失对称校验。
- 影响面：损坏文件可能被当成成功批次入湖，污染数据；是否影响下一次导入由第 4 节缓存隔离保证（计划缓存不缓存行，失败/错误行不会跨调用泄漏），但单文件静默错误本身已违反契约第 24、26 条。
- 留存：失败输入 `qa_artifacts/fail_inputs/COR-B1-ARRAY-SEG.bin`，重建参数与补丁在 evidence.json；`qa.py verify` 每次重新构造并判定，未把它计为通过案例。
- 处置：按契约不修业务代替验证，标记 **阻断**，建议在数组负段 `take` 后增加“恰好消费 size 字节”的对称校验并补回归后再放行。

## 7. 资源实测（STRESS-LONG-ARRAY）

固定种子下构造 2000 行、每行 500 个 long 的数组、8 个记录块的真实容器，测量包含返回列表、不含预先生成输入；耗时用 `time.perf_counter`，Python 分配峰值用 `tracemalloc`。本次实测以 evidence.json 的 `stress` 对象为准（每次 verify 重新测量，报告不固化机器相关数字）：行数 2000、块数 8、数组长度 500，输入字节数、完整导入耗时与分配峰值均逐次记录。契约不设机器相关门槛，此处只做如实观测。

## 8. 剩余风险

1. 本轮不建设通用语义判定器：别名、默认值、联合匹配算法仅由原 checks.py 跨版本回归覆盖，新增案例均使用读写同模式。
2. 变异仅覆盖契约指定的两类代表缺陷，不等价于完整变异测试；损坏矩阵是有限固定种子矩阵，不保证任意单字节翻转均可检出（契约也不要求）。
3. 取帧器不判定载荷语义；任何“外框合法、载荷非法”的新形态都需要像 COR-B1-ARRAY-SEG 一样由业务侧测试驱动补充。
4. 仅验证 null codec 与契约限定的数据范围；压缩、流式 feed、map/enum/fixed/递归等不在本轮范围。
5. 阻断项未修复前，数组负段损坏存在静默入湖通道；压力与其余边界结论不能抵消该风险。

## 9. 上线意见

**当前不建议上线（阻断）。** 依据：除 COR-B1-ARRAY-SEG 外，物理分块拒绝边界、合法前缀、零字节载荷、sync 一致性、失败后实例隔离与缓存计数、两类变异检出均通过且证据可重跑；但数组负段消费越界可被静默接受为错误批次，直接违背“物理损坏不得当成成功批次”的验收目标。需先在生产代码补齐数组负段恰好消费校验、用留存输入回归通过并重新执行 `python3 qa.py verify`，阻断解除后方可放行。验证流程本身（取帧器、矩阵、变异、证据与报告核对）可作为上线后的持续质量门。
