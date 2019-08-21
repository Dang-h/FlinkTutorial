package apiTest.transform

import apiTest.source.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object TransformTest {
  def main(args: Array[String]): Unit = {

	val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

	//全局并行度更改
	env.setParallelism(1)

	val streamFromFileDS: DataStream[String] = env.readTextFile("D:\\FlinkTutorial\\file\\sensor.txt")

	val dataStream: DataStream[SensorReading] = streamFromFileDS.map(
	  data => {
		val dataArray: Array[String] = data.split(",")
		SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
	  }
	)
	val dataKS: KeyedStream[SensorReading, Tuple] = dataStream.keyBy(0)
	val dataSumDS: DataStream[SensorReading] = dataKS.sum(2)


	//输出当前传感器最新温度+10 ， 时间戳时上一次数据的时间+1
	val reduceDS: DataStream[SensorReading] = dataKS.reduce {
	  (x, y) => SensorReading(x.id, y.timestamp, y.timestamp + 10)
	}

	//这里的并行数设置为1只针对print操作
	//		dataSumDS.print().setParallelism(1)

	reduceDS.print("reduceTransform")

	env.execute("Data stream")
  }
}
