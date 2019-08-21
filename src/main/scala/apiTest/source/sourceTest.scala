package apiTest.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

object sourceTest {

  def main(args: Array[String]): Unit = {

	//environment
	//创建执行环境
	val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

	//source
	//1. 从自定义集合中读取数据
	val stream1Sensor: DataStream[SensorReading] = env.fromCollection(List(
	  SensorReading("sensor_1", 1547718199, 35.80018327300259),
	  SensorReading("sensor_6", 1547718201, 15.402984393403084),
	  SensorReading("sensor_7", 1547718202, 6.720945201171228),
	  SensorReading("sensor_10", 1547718205, 38.101067604893444)
	))


	//2. 从文件中读取数据
	val stream2File: DataStream[String] = env.readTextFile("D:\\FlinkTutorial\\file\\sensor.txt")

	//3. 从Kafka中读取数据
	//配置kafka的Consumer
	val properties = new Properties()
	/*
		bootstrap.servers:kafka集群地址
		group.id：标实消费者组
		key.deserializer:实现了Deserializer的key的反序列化类
		value.deserializer:实现了Deserializer的key的反序列化类
		auto.offset.reset:当kafka的初始偏移量没了，或者当前的偏移量不存在，偏移量重置规则
	 */
	properties.setProperty("bootstrap.servers", "hadoop102:9092")
	properties.setProperty("group.id", "consumer-group")
	properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
	properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
	properties.setProperty("auto.offset.reset", "latest")

	//创建流
	//FlinkKafkaConsumer011 : kafka版本为0.11_2.11
	val stream3Kafka: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
	println("ok")


	//4. 自定义Source
	//创建类继承SourceFunction，重写cancel和run方法
	val stream4MySource: DataStream[SensorReading] = env.addSource(new SensorSource)

	//sink
	//		stream1Sensor.print("stream1").setParallelism(1)
	//		stream2File.print("stream2").setParallelism(1)
	//		stream3Kafka.print("stream3").setParallelism(1)
	stream4MySource.print("stream4")

	env.execute("source test")

  }
}

class SensorSource() extends SourceFunction[SensorReading] {

  //定义flag,表示数据源是否正常运行
  var running = true

  //正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
	//初始化一个随机数
	val random: Random = new Random()

	//初始化定义一组传感器温度数据
	//id 从1到10,并转化换结构为一个tuple
	//random.nextGaussian() * 20 : 60 ± 20~40
	val currentTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
	  i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
	)

	//死循环产生数据
	while (running) {
	  //在前一次温度上更新数据
	  val currentTempNew: immutable.IndexedSeq[(String, Double)] = currentTemp.map(
		t => (t._1, t._2 + random.nextGaussian())
	  )
	  //获取当前时间戳
	  val currentTime: Long = System.currentTimeMillis()

	  //产生数据
	  //每一个温度都包装成一个sensorReading
	  currentTempNew.foreach(
		(t: (String, Double)) => sourceContext.collect(SensorReading(t._1, currentTime, t._2))
	  )

	  //数据生产时间间隔
	  Thread.sleep(500)
	}
  }

  //取消数据源生成
  override def cancel(): Unit = {

	running = false

  }
}


//温度传感器样例类
//时间戳:10位一般时以秒为单位
case class SensorReading(id: String, timestamp: Long, temperature: Double)