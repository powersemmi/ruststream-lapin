---
title: 基准测试
description: "ruststream-lapin 在每条消息上的开销：与裸 lapin 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。"
---

import {MovedSections, RabbitMqEnvironment, RabbitMqResults} from '@site/src/components/Benchmarks';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸客户端最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "Broker", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "Broker", "rustc": "编译器", "profile": "构建配置", "features": "Feature", "rustflags": "RUSTFLAGS", "round_trip": "往返时间", "valgrind": "Valgrind"};

# 基准测试

`ruststream-lapin` 在每条消息上的开销：与裸 `lapin` 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。

## 与裸客户端对照 {/* #against-a-raw-client */}

第一张图在真实的 RabbitMQ 节点上用三种方式运行同一个场景。`裸客户端` 是一个手写的循环，直接驱动 `lapin`。`Broker crate` 是同一个循环跑在本 crate 上：Broker、队列描述符、订阅的流、投递自带的 `ack` 和它的发布者，外面没有运行时。`RustStream` 是用户写的服务：应用里的一个 `#[subscriber]` 处理器，通过同一个发布者发布。第一段差距是本 crate 比它所封装的客户端多付出的开销，第二段是运行时在其上增加的开销。

每个循环画成一条从最佳一轮到最慢一轮的横条，最佳一轮处有一个圆点，中位一轮处有一道刻痕。坐标轴的零点是裸客户端的最佳一轮，圆点落在零点右侧表示更慢。空心圆点表示该循环与裸客户端的差距小于各轮之间的波动。图下的表格给出最佳一轮，括号里是中位一轮。

两个场景对应 RabbitMQ 自带的两种队列类型。经典队列把消息放在内存里，用一个帧结算一次投递。仲裁队列先通过 Raft 复制每条消息并把它写下来，然后才交出去；这么昂贵的传输，几乎不给它上面的任何东西留下显现的余地。

## 代码的开销 {/* #cost-of-the-code */}

第二张图统计一条消息在服务线程上的开销：稳态下由 callgrind 统计的指令数和由 DHAT 统计的内存分配次数。每个场景都是跑在 `LapinBroker` 上的服务，连接同一个节点。统计范围包括框架的分发、编解码器、本 crate 的代码，以及 `lapin` 在这个线程上做的工作。`lapin` 在自己的线程上读写套接字，那个线程不计入。每一行的内存分配大多来自客户端：每一次确认、每一次发布，都会在服务线程上创建 promise 和一个内部任务。`冷启动` 是建立连接、打开信道、声明队列、注册消费者并取得第一次投递一次性付出的开销。

## 结果 {/* #results */}

<RabbitMqResults labels={labels} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<RabbitMqEnvironment labels={machine} />

这些数字来自本版本的 `docs/benchmarks/results.json`，由基准测试的 recipe 写入。[方法论](methodology.md)说明它们是怎样测得的，[数字](numbers.md)说明可以从中得出什么结论。

<MovedSections />
