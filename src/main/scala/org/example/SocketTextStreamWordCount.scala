package org.example

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object SocketTextStreamWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(30000L)

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "topic");

    val kafkaSource = new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties)

    val source = env.addSource(kafkaSource)
      //.flatMap(x => x.split(","))
      .map(elem => (elem(4).toInt, elem(5).toInt))
      //.window(TumblingEventTimeWindows.of(Time.seconds(3)))
      //.printToErr()
      //.flatMap(x => x.split(","))
      .setBufferTimeout(30000L)

    source.print()

    env.execute("test file source")


    /*kafkaConsumer.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forMonotonousTimestamps())*/

    //result.addSink(new PrintSinkFunction[String]())

    /*import org.apache.flink.streaming.api.datastream.DataStreamSink
    import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
    def print = {
      val printFunction = new PrintSinkFunction[Nothing]
      addSink(printFunction).name("Print to Std. Out")
    }*/
  }
}
