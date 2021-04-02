package com.clacjz.cityTraffic.monitor

import java.util.Properties

import com.clacjz.cityTraffic.utils.{AvgSpeedInfo, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object TopNMonitorAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id","cla_001")


    val stream: DataStream[TrafficInfo] = streamEnv.socketTextStream("node01",9999)
      .map(line => {
        var arr = line.split(",")
        new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
        override def extractTimestamp(element: TrafficInfo) = element.actionTime
      })

    stream.keyBy(_.monitorId)
      .timeWindow(Time.minutes(5),Time.minutes(1))
      .aggregate(
        new AggregateFunction[TrafficInfo,(Double,Long),(Double,Long)] {
          override def createAccumulator() = (0,0)
          override def add(value: TrafficInfo, acc: (Double, Long)) ={ (acc._1+value.speed,acc._2+1) }
          override def getResult(acc: (Double, Long)) = acc
          override def merge(a: (Double, Long), b: (Double, Long)) = {(a._1+b._1,a._2+b._2)}
        },
        (k:String, w:TimeWindow,input: Iterable[(Double,Long)],out: Collector[AvgSpeedInfo]) =>{
          val acc: (Double, Long) = input.last
          var avg :Double  =(acc._1/acc._2).formatted("%.2f").toDouble
          out.collect(new AvgSpeedInfo(w.getStart,w.getEnd,k,avg,acc._2.toInt))
        }
      )
        .assignAscendingTimestamps(_.end)
        .timeWindowAll(Time.minutes(1)) //窗口2 ，负责对窗口1输出的数据进行排序取TopN
        .apply(new AllWindowFunction[AvgSpeedInfo,String,TimeWindow] {
          override def apply(window: TimeWindow, input: Iterable[AvgSpeedInfo], out: Collector[String]): Unit = {
            var list =input.toList.sortBy(_.carCount)(Ordering.Int.reverse).take(3) //降序排序取Top3
            val sb = new mutable.StringBuilder()
            sb.append(s"在窗口${list(0).start} ----${list(0).end} 时间范围内:")
            sb.append("整个城市最拥堵的前3个卡口是:")
            list.foreach(t =>{
              sb.append(s"卡口:${t.monitorId},经过的车辆数量为:${t.carCount},平均车速为:${t.avgSpeed}")
            })
            sb.append("\n")
            out.collect(sb.toString())
          }
        })
        .print()

    streamEnv.execute()

  }
}
