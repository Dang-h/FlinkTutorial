package apiTest.sink

import java.util.Properties

import apiTest.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSink {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		env.setParallelism(1)

//		val streamFromFile = env.readTextFile("F:\\workSpace\\FlinkTutorial\\file\\sensor.txt")
		val properties = new Properties()
		/*
			bootstrap.servers:kafka集群地址
			group.id：标实消费者组
			key.deserializer:实现了Deserializer的key的反序列化类
			value.deserializer:实现了Deserializer的key的反序列化类
			auto.offset.reset:当kafka的初始偏移量没了，或者当前的偏移量不存在，偏移量重置规则
		 */
		properties.setProperty("bootstrap.servers", "sql:9092")
		properties.setProperty("group.id", "consumer-group")
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("auto.offset.reset", "latest")

		//”seneor“生产者生产数据
		val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

		val dataStream: DataStream[String] = stream.map(data => {
			val dataArray: Array[String] = data.split(",")
			SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
		})


		//”test“消费者消费数据
		dataStream.addSink(new FlinkKafkaProducer011[String]("sql:9092", "test", new SimpleStringSchema()))

		env.execute("KafkaSInk Test")
	}


}




