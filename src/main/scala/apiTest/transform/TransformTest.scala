package apiTest.transform

import apiTest.source.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object TransformTest {
  def main(args: Array[String]): Unit = {

	val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

	//全局并行度更改
//	env.setParallelism(1)

	//source
	val streamFromFileDS: DataStream[String] = env.readTextFile("D:\\FlinkTutorial\\file\\sensor.txt")

	//transform
	//1 基本转换和简单聚合
	val mapDS: DataStream[SensorReading] = streamFromFileDS.map(
	  data => {
		//对传入的一行数据按","切分==>  sensor_1  1547718199 35.80018327300259
		val dataArray: Array[String] = data.split(",")
		//切分万的数据转换为SensorReading类型,并去除两边空格===>  (sensor_1,1547718199,35.80018327300259)
		SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
	  }
	)

	//Filter,根据条件过滤
	val filterDS: DataStream[SensorReading] = mapDS.filter(id => id.id == "sensor_10")

	//将一个流拆分成不相交的分区，每个分区包含具有相同key的元素;
	//DataStream → KeyedStream
	val keyByKS: KeyedStream[SensorReading, Tuple] = mapDS.keyBy(0)

	//滚动聚合算子(Rolling Aggregation)
	//根据KeyBy分区后,对分区后数据按照第三字段"temperature"聚合
	val sumDS: DataStream[SensorReading] = keyByKS.sum(2)
	val minDS: DataStream[SensorReading] = keyByKS.min("temperature")

	//输出当前传感器最新温度+10 ， 时间戳时上一次数据的时间+1
	val reduceDS: DataStream[SensorReading] = keyByKS.reduce {
	  (x, y) => SensorReading(x.id, y.timestamp, y.timestamp + 10)
	}


	//sink
	//	mapDS.print("mapTrans")
	//	filterDS.print("FilterTrans")
	//	keyByKS.print("keyByTrans")
//	sumDS.print("sumTrans")
	minDS.print("minTrans")
	//这里的并行数设置为1只针对print操作
	//	dataSumDS.print().setParallelism(1)
	//	reduceDS.print("reduceTransform")

	//2 多流转换
	env.execute("Data stream")
  }
}
