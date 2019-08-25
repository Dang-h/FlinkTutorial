package apiTest.window

import apiTest.source.SensorReading
import apiTest.utils.MyUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//设置全局并行度为1
		env.setParallelism(1)


		//从socket中获取数据
		val inputDS: DataStream[String] = env.socketTextStream("sql", 7777)

		//		//数据处理
		//		val dataDS1: DataStream[SensorReading] = MyUtils.dataOptSimple(inputDS)
		//
		//		val minTempPerWindowByProcessingTimeDS: DataStream[(String, Double)] = dataDS1.map(data => (data.id, data.temperature))
		//		  //得到KeyedStream
		//		  .keyBy(_._1)
		//		  //windowAssigner:开窗，得到WindowStream，
		//		  //根据processing time进行划分
		//		  //对10s内相同id的数据取温度最小值
		//		  .timeWindow(Time.seconds(20))
		//		  //windowFunction
		//		  //使用reduce做增量聚合,回到DataStream
		//		  .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))


		val dataDS: DataStream[SensorReading] = MyUtils.dataOptSimple(inputDS)

		//更改时间语义为eventTime（窗口划分根据事件创建时间）且定义watermark（水位）指定时间戳
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		//指定时间戳，处理乱序数据，延迟1秒上涨水面
		val timestampDS: DataStream[SensorReading] = dataDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
			//指定时间戳
			override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
		})


		val minTempPerWindowByEventTimeDS: DataStream[(String, Double, Long)] = timestampDS.map(data => (data.id, data.temperature, data.timestamp))
		  //得到KeyedStream
		  .keyBy(_._1)
		  //对10s内相同id的数据取时间最近温度最小值
//		  	.timeWindow(Time.seconds(5))
		  // 设置滑动窗口，窗口大小为10，步长为5
		  .timeWindow(Time.seconds(10), Time.seconds(5))
		  //使用reduce做增量聚合,回到DataStream
		  .reduce((data1, data2) => (data1._1, data1._2.min(data2._2), data2._3))

		//		minTempPerWindowByProcessingTimeDS.print("min temp")
		minTempPerWindowByEventTimeDS.print("min temp")
		println()
		dataDS.print("input data")


		env.execute("Window Test")
	}

}
