package com.clacjz.cityTraffic.utils

import java.util

import org.apache.flink.configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager, HTableInterface, Put}

class HbaseWriterDataSink extends RichSinkFunction[java.util.List[Put]]{

  var conn :HConnection=_
  var conf :Configuration=_

  //初始化Hbase的链接
  override def open(parameters: configuration.Configuration): Unit = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","node04:2181")
    conn = HConnectionManager.createConnection(conf) //hbase数据库连接池
  }

  override def close(): Unit = {
    conn.close()
  }

  //往Hbase中写数据
  override def invoke(value: util.List[Put], context: SinkFunction.Context[_]): Unit = {
    val htable: HTableInterface = conn.getTable("track_info")
    htable.put(value)
    htable.close()
  }
}
