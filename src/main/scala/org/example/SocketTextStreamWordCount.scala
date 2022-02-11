package org.example

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SocketTextStreamWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaSource.builder[String]
      .setBootstrapServers("localhost:9092")
      .setTopics("records")
      .setGroupId("my-group")
      .setStartingOffsets(OffsetsInitializer.latest)
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build

    val text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
      .map(v => Integer.valueOf(v))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      .sum(0)
      .print()

    env.execute("test file source")

  }

}
