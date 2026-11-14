# Avro OCF 入湖质量契约

## 本轮范围与现有入口
本轮只验证 OCF 物理分块、损坏拒绝及失败后的实例状态隔离，不建设读写模式解析的通用语义判定器。
保留 schemas.py、codec.py、resolver.py、importer.py、fixture.py、samples.py 及 checks.py 原样；可新增测试文件，更新本契约对应的运行清单。
只使用 Python 标准库。fixture.container(writer_schema, rows, block_rows=32) 返回真实 OCF bytes；block_rows 为正整数，rows 可以为空。fixture 仅写合法容器，不提供读取规则或期望结果。
importer.Importer().import_file(path, reader_schema) 接收文件路径或位于起点的可 seek 二进制流，返回完整规范行列表。模块级 import_file 同签名。
返回行须保持文件顺序和重复项；异常时不返回部分列表，不要求输入流位置回滚。各次调用不累加行。
正常样本、错误类别和缓存语义均为被验证的业务不变量，不因本轮发现偏差而修改生产代码。

## 支持的数据范围
仅 null codec，根为 record；非递归，任一路径最多两层 record、一层 array。支持 null、boolean、int、long、string、具名 record、已定义名称引用和 union。
union 分支为原始类型或具名 record，不能包含 array 或 union，不重复分支身份；不同全名的同形 record 可以并存。不含逻辑类型、浮点、bytes、map、enum、fixed、前向引用或递归。
int/long 分别为有符号32/64位整数，bool 不作整数。原始 record 包含全部写字段，array 为列表；union 使用 {"$type":分支标识,"$value":值}，null 分支也带标签，record 分支标识为全名。
原始记录、写模式和读模式都必须合法，本轮不测试损坏的模式结构或 SchemaError 的错误先后。
为隔离物理读取问题，新增案例可令读写模式相同，独立期望直接取原始规范行；不需要另做别名、默认值或联合匹配算法。保留原样例的跨版本回归即可。
输入模式、原始记录不得被生产入口或验证器修改。

## OCF 物理格式与拒绝边界
魔数为十六进制 4f626a01，随后为元数据 map 和16字节 sync，之后零个或多个记录块。合法空容器含完整头；零字节输入是截断。
整数为 Avro long：无符号 base-128 低位组先行，最高位表示后续字节，再作 zig-zag 还原；至多10字节，结果必须在有符号64位范围。字符串/字节串以前置 long 表示字节数，长度不得为负。
元数据 map 每段先写条目数，0结束；正数段直接放对应条目，负数段以相反数为条目数，并紧跟非负字节长度，段内条目必须恰好消费该长度。每条为 UTF-8 键和字节串值。重复键拒绝。必须存在 avro.schema，其内容为 UTF-8 JSON；avro.codec 缺省视为 null，存在时必须为字节串 null。测试只给合法受支持模式或破坏其 JSON/UTF-8，不要求识别其他模式错误。
每个记录块依次为正的记录数、非负载荷字节数、恰长载荷和与头相同的16字节 sync；载荷须恰好解出声明数量的记录。全 null 字段可以使正记录数对应零字节载荷，不能一概拒绝零长度块。
数组以 long 段计数表示，0结束；负计数段还含段字节数并须恰好消费。联合索引不得越界，boolean 字节仅0或1，string 须为合法 UTF-8，int 还须满足32位范围。
测试用确定性字节变换从合法容器构造保证非法的输入，不要求任意翻转一个字节都能检出。有效内容改变不等于格式损坏。
覆盖元数据正负计数段、变长整数截断/溢出、负长度、非法块计数、声明长度/记录数不符、sync 不符、字符串 UTF-8、联合索引、数组段和不完整尾部。每份目标损坏材料都应抛 codec.DecodeError，不能返回已解析前缀；不固定生产异常消息或多处同时损坏时的错误先后。
完整头之后以及每个完整记录块末尾都是合法文件结束位置；在这些位置截短是合法前缀，预期为对应前缀行，不能误报损坏。长度框定的载荷中可以包含与 sync 相同的字节，不能用搜索 sync 代替取帧。
本轮不引入非 null 压缩、流式 feed、网络、数据库或生产功能修复。

## 独立取帧入口
新增 frame_oracle.py，公开 inspect(blob)，blob 为完整 bytes。返回 JSON 可序列化对象：
{"header_end":整数,"sync":小写十六进制字符串,"blocks":[{"start":整数,"count":整数,"payload_start":整数,"payload_end":整数,"end":整数,"sync_ok":true}]}
所有偏移从0计，范围左闭右开。header_end 在头 sync 后，start 在块计数前，payload_start/payload_end 只界定载荷，end 在块 sync 后；空容器 blocks=[]。
外框非法时抛该模块的 OracleError，具有 code 和 offset 属性。code 使用 magic、varint、metadata、length、count、sync、truncated；offset 为相关字段起始字节，读取不足时为首个缺失字节即 len(blob)。完整字段的 long 溢出归 varint，已完整解出的负长度归 length，完整的非正记录块计数归 count，元数据内容或段消费不符归 metadata；单一故障案例按此分类，不要求多故障的统一优先级。
inspect 独立检查头、元数据和外层块边界；记录载荷视为不透明字节，不解析模式图或记录值，也不判定正记录数与实际载荷是否匹配。外框合法而载荷非法时，inspect 可以成功，测试驱动须依据已知构造另行判定生产入口应拒绝，报告不得声称取帧器验证了记录语义。
frame_oracle 不得导入或调用 schemas、codec、resolver、importer、fixture 的解析、编码、规范化或判定函数，也不得使用其他 Avro 读取器。测试驱动可以调用生产入口及 fixture。
用手工可核对的微型容器和预先算出的偏移、计数交叉核验取帧器，不能只让它与生产解析器相互背书。构造输入的辅助编码逻辑也不能作为取帧器的读取逻辑。

## 失败后的缓存与继续读取
Importer.hits/misses 是实例内累计计划查找计数，缓存键由写模式和读模式规范内容共同决定。内容相同但对象不同或字段字典键次序不同不产生新计划。
完整头解析失败时不查计划，计数及原有计划不变。头成功、读写模式合法时，在读取记录前恰好进行一次计划查找：已有计划 hits 加1，新计划 misses 加1并缓存；之后记录失败不撤销该计划或计数。空容器同样进行查找。
clear_cache() 清除计划并把 hits、misses 都置0；之后合法调用重新 miss。
同一实例在头失败、首块失败及合法前缀后的块尾失败后继续导入合法文件，结果须等于冷启动；热缓存、交换文件次序和 clear_cache 后也须成立。失败不得留下可被下一次调用返回的旧行。
测试应核对每次调用后的计数，不只看最终总数；不用私有 _plans 结构作为验收接口。

## 质量交付与重跑
新增 qa.py，支持 python3 qa.py verify 和 python3 qa.py demo。
verify 先重跑 python3 checks.py，再运行独立取帧互核、有限固定种子的合法/非法边界矩阵、缓存隔离、两类变异和一组长数组多块压力，重新生成 evidence.json 并核对 REPORT.md 的案例引用及结论。
合法矩阵覆盖空容器、单块、多块、不同 block_rows、重复行、数组/联合载荷和合法前缀，逐行核对期望与实际；损坏矩阵包含首部、首块及已有合法前缀后的失败。不要建设通用模糊测试平台或全模式语义测试器。
仅在隔离副本或可撤销补丁内执行两类变异：忽略块 sync 校验；吞掉块尾 DecodeError 并成功返回已解出的前缀。每类须实际运行，并留下成功返回错误业务结果的输入与期望/实际见证；导入失败、语法错误或普通异常退出不算检出。
evidence.json 的结构自行设计，须保存案例ID、合法源模式/记录或可重建参数、固定种子、字节补丁及原始偏移、独立帧摘要或错误位置、期望/实际、逐调用缓存计数和变异见证。未变换输入可用空补丁。
压力记录行数、块数、数组长度、输入字节数、完整导入调用耗时与 Python 分配峰值；测量包含返回列表，不含预先生成的输入。不设机器相关耗时或内存门槛，也不要求优化实现。
REPORT.md 引用证据案例，说明源码边界、取帧器的判定范围、覆盖依据、失败隔离结论、变异检出、资源实测、剩余风险与上线意见。verify 不得仅读旧证据或搜索成功标记宣告通过。
验证器错误、变异未被业务判据检出、证据与报告不一致必须使 verify 非零；生产偏差须保存可重跑失败输入和实际结果，在报告中标记阻断，不要求修复，也不得计为通过案例。验证工作完整且证据一致时输出 AVRO_FRAME_QA_OK，可携带如实核定的阻断结论。
demo 输出真实容器的帧摘要、生产导入行及一个损坏案例的期望/实际，不只打印成功标记。
把 .pointwise-runtime.json 的 verify/demo 分别改为 ["python3","qa.py","verify"] 和 ["python3","qa.py","demo"]，保留 version=1、kind=cli、setup=[]。

## 已有回归与样例
python3 checks.py 成功输出 AVRO_NORMAL_OK；python3 checks.py demo 还显示两个真实版本的导入结果，原断言须保留。
python3 samples.py OUTDIR 写出 v1.avro、v2.avro、reader.json。samples.files() 返回相同容器 bytes，便于直接以 io.BytesIO 调用。
以 samples.READER 读取 v1 首行应为 {"id":"a","value":7,"note":{"$type":"null","$value":null},"tags":[]}；
v2 首行应为 {"id":"c","value":12,"note":{"$type":"null","$value":null},"tags":["ok"]}。
初始运行清单 verify/demo 指向已有回归及演示，完成本轮质量交付后再切到 qa.py。
