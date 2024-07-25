# MitCS6.824分布式系统
## MapReduce
### 介绍
![p1](img/MapReduce%20Framework.png)
- MapReduce是一种分布式计算模型，由Google提出，主要用于搜索领域，解决海量数据的计算问题
- MR由两个阶段组成：Map和Reduce，用户只需要实现map()和reduce()两个函数，即可实现分布式计算，非常简单
- 这两个函数的形参是key、value对，表示函数的输入信息
### 执行步骤：
  - 1. map任务处理
    - 1.1、读取输入文件内容，解析成key、value对；对输入文件的每一行，解析成key、value对；每一个键值对调用一次map函数 
    - 1.2、写自己的逻辑，对输入的key、value处理，转换成新的key、value输出 
    - 1.3、对输出的key、value进行分区
    - 1.4、对不同分区的数据，按照key进行排序、分组。相同key的value放到一个集合中
    - 1.5、分组后的数据进行归约 
  - 2. reduce任务处理 
    - 2.1、对多个map任务的输出，按照不同的分区，通过网络copy到不同的reduce节点    
    - 2.2、对多个map任务的输出进行合并、排序。写reduce函数自己的逻辑，对输入的key、value处理，转换成新的key、value输出 
    - 2.3、把reduce的输出保存到文件中
  - 3. 任务协调
    - 需要使用一个线程对任务进行分配与回收
    - Master与 Worker 之间使用 RPC 进行通信
### 任务分析
- 整个MR框架由一个 Coordinator 进程及若干个Worker进程构成
- master 和 worker 传递信息通过 *channel* 来实现、
- 需要设置一个函数来实现 worker 对 master 的任务申请（ApplyForTask）
- 需要设置一个结构体来保存对申请任务的参数 (ApplyForTaskArgs)
- 需要设置一个结构体来对任务结构进行说明 (Task)
- 需要设置一个结构体来返回任务 (ApplyForTaskReply)
- 使用文件来保存中间结果
- 在启动时根据指定的输入文件数及Reduce Task数，生成Map Task及Reduce Task 
- 响应Worker的Task申请RPC请求，分配可用的Task给到Worker处理 
- 追踪Task的完成情况，在所有Map Task完成后进入Reduce阶段，开始派发Reduce Task；在所有Reduce Task完成后标记作业已完成并退出
