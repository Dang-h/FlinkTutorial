package apiTest.transform

import apiTest.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object TransformTest {
	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//全局并行度更改
		env.setParallelism(1)

		//source
		//		val streamFromFileDS: DataStream[String] = env.readTextFile("D:\\FlinkTutorial\\file\\sensor.txt")
		val streamFromFileDS: DataStream[String] = env.readTextFile("F:\\workSpace\\FlinkTutorial\\file\\sensor.txt")

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
		val minByDS: DataStream[SensorReading] = keyByKS.minBy(2).setParallelism(1)
		//输出当前传感器最新温度+10 ， 时间戳是上一次数据的时间+1
		val reduceDS: DataStream[SensorReading] = keyByKS.reduce {
			//两个时间点：当前，上一次
			(x: SensorReading, y: SensorReading) => SensorReading(x.id, x.timestamp + 1, y.timestamp + 10)
		}

		//2 多流转换算子
		//DataStream → SplitStream：根据某些特征把一个DataStream拆分成两个或者多个DataStream
		val splitSS: SplitStream[SensorReading] = mapDS.split(data => {
			if (data.temperature > 30) {
				//Seq trait用于表示序列。
				// 所谓序列，指的是一类具有一定长度的可迭代访问的对象，其中每个元素均带有一个从0开始计数的固定索引位置。
				Seq("high")
			} else {
				Seq("low")
			}
		})
		//SplitStream→DataStream：从一个SplitStream中获取一个或者多个DataStream。
		val high: DataStream[SensorReading] = splitSS.select("high")
		val low: DataStream[SensorReading] = splitSS.select("low")
		val all: DataStream[SensorReading] = splitSS.select("high", "low")

		//合并两条流

		//将温度>30的数据转换结构为（id，temperature）
		val highTempDS: DataStream[(String, Double)] = high.map(data => (data.id, data.temperature))
		//	DataStream,DataStream → ConnectedStreams：连接两个保持他们类型的数据流，
		//	两个数据流被Connect之后，只是被放在同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
		val connectCS: ConnectedStreams[(String, Double), SensorReading] = highTempDS.connect(low)

		// 将整合的流按要求转换格式输出
		// !!!! 调用的函数有两个或者两个以上的参数，只能使用（） !!!
		val connectMapDS: DataStream[Product] = connectCS.map(
			(highData: (String, Double)) => (highData._1, highData._2, "warning"),
			(lowData: SensorReading) => (lowData.id, "OK")
		)

		//粗暴合并
		val unionDS: DataStream[SensorReading] = high.union(low)


		//3 UDF
		//	调用函数只有但一参数，（）可以省略
		//	mapDS.filter(new MyFilter).print()
		//富函数，匿名函数使用
		mapDS.filter(
			new RichFilterFunction[SensorReading] {
				override def filter(t: SensorReading): Boolean = {
					t.id == "sensor_10"
				}
			}
		).print()


		//sink
		//	mapDS.print("mapTrans")
		//	filterDS.print("FilterTrans")
		//	keyByKS.print("keyByTrans")
		//	sumDS.print("sumTrans")
		//这里的并行数设置为1只针对print操作
		//	minDS.print("minTrans").setParallelism(1)
		//	minByDS.print("minByTrans")
		//	reduceDS.print("reduceTrans")
		//	high.print("splitTransHigh")
		//	low.print("splitTransLow")
		//	all.print("splitTransAll")
		//	highTempDS.print("highMapTrans")
		//	connectMapDS.print("connectTrans")
		//	unionDS.print("unionTrans")


		env.execute("Data stream")
	}
}


// 自定义函数，继承相应的类，重写对应的方法
class MyFilter() extends FilterFunction[SensorReading] {
	override def filter(t: SensorReading): Boolean = {
		t.id.startsWith("sensor_10")
	}
}