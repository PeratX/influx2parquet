# 从 12 盘 RAID 到 Parquet：一次 InfluxDB 市场数据恢复与导出的完整记录

很多数据恢复项目，最后停在“阵列能挂出来了”这一步。

但这次不是。

这次真正想要的，不是一套老存储硬件本身，而是里面那份 InfluxDB `OptionData` 数据库，以及数据库里那些还能继续被分析、被研究、被复用的市场数据。

所以这篇文章想记录的，不只是一次 RAID 恢复，也不只是一次数据库导出，而是一条完整的链：

- 从 `LSI 2308 HBA + expander + 12x 10TB HC510` 的硬件入口开始
- 到用 `UFS Explorer` 重新判断磁盘顺序和 RAID 参数
- 再到通过 `iSCSI` 暴露逻辑卷
- 最后把 InfluxDB 里的核心 measurement 导出成 Parquet

中间还穿插着很多很现实的问题：

- 原始 RAID 卡不在
- 阵列参数未知
- 旧硬件要不要继续留
- Influx 的常规导出为什么对深度簿数据特别低效
- 空间不够时，导出流水线应该怎么拆

这不是一篇“完美环境下的教程”，更像是一篇现实世界里的工程手记。

## 一、硬件起点：不是 RAID 卡，而是 HBA + Expander

这次恢复的存储基础是：

- `LSI 2308 HBA`
- expander 背板
- `12 x 10TB WD HC510 Helium HDD`

如果原始 RAID 卡还在，很多事情会简单得多：插回去，读元数据，卷可能就直接出来了。

但现实是，原控制器不在，阵列参数也不清楚。这时候 HBA 反而变成了一个更好的起点，因为它至少能把每块盘干净地暴露出来，让后续的识别工作回到“底层事实”上，而不是依赖某个已经不在场的控制器。

这类 12 盘机械阵列还有一个不能回避的问题：散热。

恢复和导出都属于长时间连续顺序读。盘数多、密度高、时间长，温度管理就是第一优先级。实际运行里，用一台外部电风扇直吹阵列，把 12 块 `HC510` 长时间稳定压在 `36-40°C`。对这种长时间扫描负载来说，这已经是很舒服的区间。

很多人会觉得这类方案“不够专业”，但工程上看结果就够了：温度稳、盘不热、任务能持续跑完，它就是对的。

## 二、没有原 RAID 卡，先靠 UFS Explorer 把阵列拼回来

这次问题的本质，不是盘坏得一塌糊涂，而是：

- 原 RAID 卡没了
- 原阵列参数不知道
- 但盘上的数据大概率还在

这类场景下，真正重要的不是“赶紧挂起来试试”，而是先把阵列结构搞清楚。

这里起关键作用的工具是 `UFS Explorer`。

UFS Explorer 的价值，不只是“能看文件”，而是它可以帮助恢复人员一步一步推断：

- 盘序
- RAID level
- 条带大小
- 起始偏移
- 奇偶布局
- 其他元数据细节

如果把原阵列想成一张已经被拆散的拼图，那么 UFS Explorer 做的事情，就是把这张拼图从“盲猜”变成“有证据地重建”。

这一步很花耐心，但也是整件事里最不能省的一步。因为后面所有工作，无论是挂载、镜像、还是数据库导出，都建立在“逻辑卷被正确重建”这个前提上。

## 三、通过 iSCSI 把旧硬件和后处理工作拆开

当逻辑卷被正确识别出来以后，我没有把所有后续工作都继续留在接盘机器上做，而是走了一条更灵活的路：

- 用 `UFS Explorer` 负责识别和重组
- 再通过 `iSCSI` 把逻辑卷暴露给后续处理环境

这一步的意义非常大，因为它把问题拆成了两层：

- 一层负责“理解旧硬件和旧阵列”
- 一层负责“处理数据本身”

也就是说，旧机器不再承担整个项目，只承担：

- 接盘
- 识别卷
- 暴露卷

而真正的数据导出、校验、转换、分析，则可以放到更适合跑脚本和做数据工程的环境里。

如果是更脆弱、更偏取证的场景，通常建议先做本地镜像，再远端处理。但这次阵列本身并没有到那个地步，`UFS Explorer + iSCSI` 这个组合刚好足够实用。

## 四、两套桌面平台：前半段和后半段用的不是同一台机器

这次项目里，一个很有意思的细节是：导出过程并不是始终跑在同一套桌面平台上。

### 前期平台

前半段，大多数 measurement 的导出跑在一套更新的平台上：

- `i5-14600KF`
- `32GB DDR5`
- 虚拟机分配：
  - `6` 个 P-Core
  - `24GB` DRAM

这套配置更适合处理前期那些“常规 measurement + HTTP/Parquet 导出”的工作。因为那一阶段：

- measurement 多
- 流程比较通用
- Python + `pyarrow` 路线已经够用

### 后期平台

到了最棘手的 `okex_depth` 阶段，平台换成了另一套更便宜但更务实的机器：

- `i7-10700`
- `16GB DDR4`
- 虚拟机分配：
  - `8` 个物理核心
  - `12GB` DRAM

这看起来像是“降级”，但对 `okex_depth` 来说其实很合理。因为这部分真正的瓶颈，不是纯 CPU 绝对性能，而是：

- TSM 读取
- 中间压缩
- 磁盘空间
- 数据流水线结构

当 direct TSM 路线跑通以后，`10700 + 16GB DDR4` 这种平台已经足够胜任，而且更符合“把眼前任务做完，再处理硬件资产”的思路。

这件事也让我更确定：硬件不是为了维持某种配置自尊而存在的，它应该服从任务本身。

## 五、恢复出的不是文件夹，而是一份 InfluxDB `OptionData`

逻辑卷出来以后，真正的目标才开始显形：里面是一份 InfluxDB 的 `OptionData` 数据库。

而这份数据库里，并不是所有 measurement 都值得保留。

最后确定的核心 keep-list 是：

- `binance_depth`
- `binance_ticker`
- `binance_trade`
- `okex_index_ticker`
- `okex_ticker`
- `okex_trades`
- `candlestick`
- `perpetual`

可选的深度数据是：

- `binance_depth20`
- `okex_depth`

其中最麻烦的就是 `okex_depth`，因为它存的是原始深度 payload。它不只是“多一个 measurement”，而是整个数据库里最难导、最占空间、也最难取舍的一类数据。

## 六、常规导出：HTTP 查询再写 Parquet，够用但不优雅

对于大多数 measurement，最直接的路线还是：

1. 连上 InfluxDB HTTP 接口
2. 按 measurement 流式查询
3. 用 Python 把结果写成 Parquet dataset

仓库里对应的脚本是：

- [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)

这条路的优点很明确：

- 思路简单
- 适用于大多数普通 measurement
- 支持 checkpoint，失败后容易续跑

所以 keep-list 里的很多数据，都是靠这条路稳定完成的。

从工程角度说，这类脚本的价值恰恰不在“先进”，而在“清楚、稳定、够用”。很多时候，这才是最重要的。

### 已经完成的结果

到目前为止，核心 keep-list 里除了 `okex_depth` 之外，常规数据已经基本导出完成。  
下面这组数字来自实际 Parquet 数据集的落盘统计：

| measurement | rows | actual | uncompressed | ratio |
| --- | ---: | ---: | ---: | ---: |
| `binance_depth` | 347,434,355 | 5.76 GB | 10.96 GB | 1.90x |
| `binance_depth20` | 233,524,155 | 16.47 GB | 211.26 GB | 12.82x |
| `binance_ticker` | 169,157,156 | 5.81 GB | 11.57 GB | 1.99x |
| `binance_trade` | 622,555,705 | 13.34 GB | 33.64 GB | 2.52x |
| `candlestick` | 25,676 | 459.46 KB | 842.35 KB | 1.83x |
| `okex_index_ticker` | 200,289,937 | 1.38 GB | 2.23 GB | 1.62x |
| `okex_ticker` | 614,378,499 | 9.75 GB | 18.41 GB | 1.89x |
| `okex_trades` | 553,194,234 | 5.98 GB | 16.82 GB | 2.81x |
| `perpetual` | 1,863,864 | 29.41 MB | 50.89 MB | 1.73x |

这里的 `ratio` 不是“相对原始 TSM 的压缩比”，而是：

- Parquet 元数据里记录的未压缩列大小
- 对比最终实际落盘大小

也就是说，它描述的是 **Parquet 编码后再经过压缩 codec 的收益**。

这组结果里最有意思的有两点：

- `binance_depth20` 的压缩收益极高，达到 `12.82x`。这类深度簿数组数据在列式存储和 ZSTD 下有非常明显的重复模式。
- `trade` 类数据通常比 `ticker` 更好压，因为字段重复更多、时间序列模式更稳定，所以 `binance_trade` 和 `okex_trades` 的压缩比都明显高于 `ticker`。

从结果上看，常规 measurement 这条“Influx 查询 -> Parquet”路线已经把大部分主数据集稳稳落下来了。真正剩下最难啃的，反而就是那个不适合继续走老路的 `okex_depth`。

## 七、`okex_depth` 为什么不能继续走老路

`okex_depth` 和大多数 measurement 的区别，不只是“量更大”，而是它的数据形态本身就非常不友好：

- `asks`
- `bids`
- `action`
- `checksum`
- `ts`

其中最重的是 `asks` / `bids`，它们本来就是很长的字符串 payload。

如果继续走传统 Influx 导出路径，就会发生一件特别蠢的事情：

- TSM 里的紧凑二进制块被展开成文本
- 文本再被 Python 重新 parse
- parse 完再写中间文件
- 最后再压成 Parquet

对这种深度簿数据来说，text serialization / deserialization 就是一笔巨额性能税。CPU、内存、I/O 不是在搬数据，而是在反复做“翻译”。

## 八、Python 版 direct TSM：先绕开 HTTP，再直接打 TSM

所以 `okex_depth` 最初采用的不是 HTTP 查询，而是 direct TSM 流水线：

- [python/export_okex_depth_tsm.py](python/export_okex_depth_tsm.py)

它把流程拆成四个可恢复阶段：

1. `scan`
2. `export`
3. `merge`
4. `build`

这已经比 HTTP 导出健康很多了。因为至少它绕开了 Influx HTTP/JSON 查询层，直接从 `.tsm` 文件入手。

但 Python 版还有一个根本问题：它虽然直读 TSM 的来源，最后还是借助 `influx_inspect export` 把内容展开成 line protocol 文本，然后再被 Python 解析回结构化数据。

也就是说，它虽然绕开了 HTTP，但还没有绕开文本本身。

## 九、真正的转折：Go 版 direct TSM/WAL，把文本中间层整个砍掉

后面最重要的变化，是仓库里出现了 Go 版 direct TSM/WAL 导出器：

- [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)

它和 Python 版最大的不同，是不再依赖 `influx_inspect export` 的 line protocol 文本链路，而是：

- 直接读取 TSM block
- 对最后一个 shard 的 WAL 做补齐
- 直接写二进制 spool 中间层

这一步的收益非常明显：

- 内存占用大幅下降
- 不再有 stdout/pipe 搬文本的瓶颈
- CPU 更集中在真实压缩和真实导出上
- 同样是 2GB 左右的 TSM 文件，耗时从过去的小时级，掉到了分钟级

如果说前面的路径是在“尽量绕开 Influx 的常规查询”，那么 Go 版才是真正把这条链路改造成了一条适合重度导出的数据流水线。

## 十、为什么新流水线还是保留 `export -> merge -> build`

很多人看到新的 Go 导出器以后，第一反应会是：既然已经这么快了，为什么不直接 TSM -> Parquet 一步到位？

答案是不能这么简单。

因为这三件事其实是不同问题：

- `export`
  解决的是“把每个 source 的字段流抽出来”
- `merge`
  解决的是“同一字段跨 source 合并、排序、去重”
- `build`
  解决的是“把多个字段重新拼成最终行，再写成 Parquet”

这就是为什么流水线最终保留了三层目录：

- `_raw_go`
- `_merged_go`
- 最终 `<instId>.parquet/`

中间层的代价是占空间，但换来的好处是：

- 流水线可恢复
- 参数可调整
- 结果可检查
- 某一阶段失败时，不需要从头来过

这笔账是值得的。

## 十一、空间从来不是旁枝末节，而是主战场

`okex_depth` 真正跑起来以后，一个非常现实的问题是：空间不够。

所以这次后面又做了几轮很务实的优化：

- 提高中间层 `zstd` 压缩等级
- 把 Go 流水线拆成独立工作区：
  - `raw`
  - `merged`
  - `build`
  - `final`
- 允许把不同阶段放到不同磁盘上

这听起来像是“目录工程”，其实不是。

它的实际意义是：

- export 可以继续往一块盘写
- merge/build/final 可以切到另一块更大的 SSD
- 最终在空间极限附近，任务还能继续跑下去

这种时候，漂亮的目录结构不重要，重要的是磁盘不会在半夜被写满。

## 十二、删不删 raw 不是拍脑袋，要在 merge 之后做检查

当 `_raw_go` 很大、磁盘空间又紧张的时候，一个自然的问题就是：merge 既然已经完成了，raw 能不能删？

答案是：可以，但应该先检查。

所以后来又专门补了一个脚本：

- [python/check_okex_depth_merge_before_raw_cleanup.py](python/check_okex_depth_merge_before_raw_cleanup.py)

它做的事情不是删除，而是验证：

- export 是否完整完成
- merge 状态是否完整
- `_merged_go` 的行数、时间序和状态是否一致
- 如有需要，还能重放 raw 去做更强校验

只有检查通过以后，再手动删 `_raw_go`，这才是更安全的做法。

## 十三、恢复环境本身也会制造污染，所以要主动管理

还有一个很容易被忽略的问题是：恢复环境自己会不会继续往上层目录里生成新数据。

这在 InfluxDB 上尤其明显。

如果把恢复出来的数据库挂在 overlay 上，然后让 `influxdb` 在这个环境里持续运行，它可能会：

- 继续 compaction
- 继续生成 `.tsm` / `.tsm.tmp`
- 把 `/opt/upper/data` 很快写爆

所以仓库里有一个非常重要的辅助脚本：

- [scripts/reset_optiondata_stack.sh](scripts/reset_optiondata_stack.sh)

它负责重新搭建恢复环境，并且默认让 `influxdb` 保持停止。这样 direct TSM 路线就能在一个更干净的环境里工作，不会一边恢复一边自己制造新污染。

## 十四、最后得到的不是“修好的阵列”，而是可用的数据资产

如果只从硬件角度看，这件事可能会被概括成：

- 一张 `LSI 2308`
- 一个 expander
- 12 块 `HC510`
- 一套通过 `UFS Explorer` 重新识别出的 RAID

但从结果上看，真正重要的并不是这些硬件还在不在，而是：

- InfluxDB `OptionData` 被成功取出
- 核心 measurement 被筛选出来
- 常规 measurement 被整理成了标准 Parquet
- 最难的 `okex_depth` 也有了高吞吐、可恢复、可检查的 direct TSM/WAL 导出路径

这时候，阵列、HBA、旧桌面、旧 SSD，才真正退回到它们本来的位置：工具。

它们的价值，不在于继续被供起来，而在于帮任务完成。

## 十五、如果从今天回头看，最重要的经验是什么

如果把整件事压缩成几句最重要的话，大概是：

### 1. 先解决问题，再追求形式

能把 12 盘 RAID 稳定吹在 `36-40°C` 的电风扇，就是好风扇。

能正确识别盘序和 RAID 参数的 `UFS Explorer`，就是对的工具。

能把 line protocol 整层砍掉的 Go 导出器，就是该走的方向。

### 2. 文本中间层在大数据导出里是重税

line protocol、JSON、TSV 都很适合 debug，但对 TB 级数据搬运极不友好。

尤其对深度簿这种超长 payload，文本化基本等于主动交性能税。

### 3. 中间层不是原罪，失控才是

`raw`、`merged`、`build` 分层本身没有问题。真正的问题是：

- 能不能恢复
- 能不能验证
- 能不能在合适的时机删掉

### 4. 硬件应该服务任务，而不是反过来

这次项目里，两套桌面平台的切换本身就是一个很好的提醒：

- 前期用 `14600KF + DDR5`
- 后期换到 `10700 + DDR4`

看起来像配置退步，实际上只是让硬件重新回到了“服务任务”的位置。

如果旧平台已经足够把事做完，那它就够了。真正该保住的是数据，不是硬件自尊。

## 十六、仓库里哪些文件和这篇文章对应

如果有人是从这篇文章反过来想看代码，对应关系大致是：

- 通用导出：
  - [python/extract_optiondata_to_parquet.py](python/extract_optiondata_to_parquet.py)

- 并发按 `instId` 导出：
  - [python/export_parallel_instid.py](python/export_parallel_instid.py)

- Python 版 direct TSM：
  - [python/export_okex_depth_tsm.py](python/export_okex_depth_tsm.py)

- Go 版 direct TSM/WAL：
  - [go/okex_depth_direct/cmd/okex_depth_direct/main.go](go/okex_depth_direct/cmd/okex_depth_direct/main.go)

- 新旧 raw 对比：
  - [python/compare_okex_depth_raw.py](python/compare_okex_depth_raw.py)

- merge 完成后的 raw 清理前检查：
  - [python/check_okex_depth_merge_before_raw_cleanup.py](python/check_okex_depth_merge_before_raw_cleanup.py)

- 恢复环境重置：
  - [scripts/reset_optiondata_stack.sh](scripts/reset_optiondata_stack.sh)

## 结语

这次事情最有意思的地方，是它很难被简单归类。

它不是一篇纯硬件文章，也不是一篇纯数据库文章，更不是一篇单纯的数据工程记录。

它更像一次现实世界里的混合任务：

- 一部分是老硬件恢复
- 一部分是阵列识别
- 一部分是系统挂载和网络暴露
- 一部分是 InfluxDB 数据导出
- 最后又变成了一次关于 text pipeline、binary spool、压缩、空间管理和 Parquet 的工程优化

也正因为这样，它才值得被写下来。

因为真正困难的项目，往往都不是“某一个点很难”，而是每一层都带一点现实世界的脏活，然后它们一起堆在你面前。

这次的终点，不是一套还能亮机的旧阵列。

终点是：**把一份本来困在旧存储和旧数据库里的市场数据，重新变回可用的数据资产。**
