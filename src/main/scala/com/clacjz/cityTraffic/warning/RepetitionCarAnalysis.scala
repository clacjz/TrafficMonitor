package com.clacjz.cityTraffic.warning

import java.util.Properties

import com.clacjz.cityTraffic.utils.{RepetitionCarWarning, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 套牌车分析，从数据流中找出存在套牌嫌疑的车辆
 */
object RepetitionCarAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id","msb_001")

    val stream: DataStream[TrafficInfo] = streamEnv.socketTextStream("node01",9999)
      .map(line => {
        var arr = line.split(",")
        new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      }) //引入时间时间
      .assignAscendingTimestamps(_.actionTime)

    stream.keyBy(_.car)
      .process(new KeyedProcessFunction[String,TrafficInfo,RepetitionCarWarning] {
        //第一辆出现的信息对象
        lazy val firstState = getRuntimeContext.getState(new ValueStateDescriptor[TrafficInfo]("first",classOf[TrafficInfo]))

        override def processElement(i: TrafficInfo, context: KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning]#Context, collector: Collector[RepetitionCarWarning]) = {
          val first :TrafficInfo =firstState.value()
          if(first==null){ //第一辆车
            firstState.update(i)
          }else{ //时间超过了10秒？
            val nowTime = i.actionTime
            val firstTime = first.actionTime
            var less:Long = (nowTime - firstTime).abs /1000
            if(less<=10){ //涉嫌
              var warn =new RepetitionCarWarning(i.car, if(nowTime>firstTime) first.monitorId else i.monitorId,
                if(nowTime<firstTime) first.monitorId else i.monitorId,
                "涉嫌套牌车",
                context.timerService().currentProcessingTime()
              )
              collector.collect(warn)
              firstState.clear()
            }else{ //不是套牌车
              if(nowTime>firstTime) firstState.update(i)
            }
          }
        }
      } )
      .addSink(new WriteDataSink[RepetitionCarWarning](classOf[RepetitionCarWarning]))

    streamEnv.execute()
  }
}
