package com.clacjz.cityTraffic.warning

import java.sql.DriverManager
import java.util
import java.util.Properties

import com.clacjz.cityTraffic.utils._
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


object  CarTrackInfoAnalysis{

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)

    val stream1: DataStream[TrackInfo] = streamEnv.readTextFile("删去")
    .map(line => {
      var arr = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })
    .filter(new MyViolationRichFilterFunction())
    .map(info =>{
      new TrackInfo(info.car,info.actionTime,info.monitorId,info.roadId,info.areaId,info.speed)
    })

  stream1.countWindowAll(10)
  .apply(
    (win:GlobalWindow,input:Iterable[TrackInfo],out: Collector[java.util.List[Put]]) =>{
      var list = new util.ArrayList[Put]()
      for(info<-input){
        //rowkey 车牌号+ (Long.maxValue - actionTime)
        var put = new Put(Bytes.toBytes(info.car+"_"+(Long.MaxValue - info.actionTime)))
        put.add("cf1".getBytes(),"car".getBytes(),Bytes.toBytes(info.car))
        put.add("cf1".getBytes(),"actionTime".getBytes(),Bytes.toBytes(info.actionTime))
        put.add("cf1".getBytes(),"monitorId".getBytes(),Bytes.toBytes(info.monitorId))
        put.add("cf1".getBytes(),"roadId".getBytes(),Bytes.toBytes(info.roadId))
        put.add("cf1".getBytes(),"areaId".getBytes(),Bytes.toBytes(info.areaId))
        put.add("cf1".getBytes(),"speed".getBytes(),Bytes.toBytes(info.speed))
        list.add(put)
      }
      print(list.size()+",")
      out.collect(list)
    }
  )
  .addSink(new HbaseWriterDataSink)

    streamEnv.execute()

  }

  //留下违法车辆信息
  class MyViolationRichFilterFunction extends RichFilterFunction[TrafficInfo]{
    //map集合违法车辆信息
    var map  = scala.collection.mutable.Map[String,ViolationInfo]()
    //一次性从数据库中读所有的违法车辆信息列表存放到Map集合中，open函数在计算程序初始化的时候调用的，
    override def open(parameters: Configuration): Unit = {
      var conn = DriverManager.getConnection("jdbc:mysql://localhost/traffic_monitor","root","123123")
      var pst = conn.prepareStatement("select car ,violation, create_time from t_violation_list")
      var set =pst.executeQuery()
      while (set.next()){
        var info =new ViolationInfo(set.getString(1),set.getString(2),set.getLong(3))
        map.put(info.car,info)
      }
      set.close()
      pst.close()
      conn.close()
    }

    override def filter(t: TrafficInfo): Boolean = {
      val o: Option[ViolationInfo] = map.get(t.car)
      if(o.isEmpty){
        false
      }else{
        true
      }
    }
  }
}
