package com.clacjz.cityTraffic.warning

import com.clacjz.cityTraffic.utils.ViolationInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//出警记录对象
case class PoliceAction(policeId: String, car: String, actionStatus: String, actionTime: Long)


object ViolationCarAndPoliceActionAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val stream1: DataStream[ViolationInfo] = streamEnv.socketTextStream("hadoop101", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new ViolationInfo(arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationInfo](Time.seconds(2)) {
        override def extractTimestamp(element: ViolationInfo) = element.createTime
      })

    val stream2: DataStream[PoliceAction] = streamEnv.socketTextStream("hadoop101", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new PoliceAction(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PoliceAction](Time.seconds(2)) {
      override def extractTimestamp(element: PoliceAction) = element.actionTime
    })

    stream1.keyBy(_.car)
      .intervalJoin(stream2.keyBy(_.car))
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new ProcessJoinFunction[ViolationInfo, PoliceAction, String] {
        override def processElement(left: ViolationInfo, right: PoliceAction, ctx: ProcessJoinFunction[ViolationInfo, PoliceAction, String]#Context, out: Collector[String]) = {
          out.collect(s"车辆${left.car},已经有交警出警了，警号为:${right.policeId},出警的状态是：${right.actionStatus},出警的时间:${right.actionTime}")
        }
      })
      .print()
    streamEnv.execute()
  }
}
