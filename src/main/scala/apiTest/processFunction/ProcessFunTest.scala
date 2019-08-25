package apiTest.processFunction

import apiTest.source.SensorReading
import apiTest.utils.MyUtils
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//3秒之内温度连续上升就报警
object ProcessFunTest {

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		val inputDS: DataStream[String] = env.socketTextStream("sql", 7778)

		val dataStream: DataStream[SensorReading] = MyUtils.dataOptSimple(inputDS)
		  .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
			  override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
		  })

		//直击底层，啥都能拿到，解决一切需求：牛牪犇
		val processDS: DataStream[String] = dataStream.keyBy(_.id)
		  .process(new TempIncreAlert())


		dataStream.print("input data")
		processDS.print("processed data")


		env.execute("processFunction Test")

	}

}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{
	/*判断温度连续上升，当前温度和上一个温度比较，
	3秒连续上升报警：3秒可以调用时间服务指定一个定时器，3秒后触发，出发后判断温度是否连续上升
	获取上一次的温度：保存成当前的状态
	*/

	//定义一个状态用来保存上一个数据的温度
	//lazy：懒加载，定义的时候不执行，调用时候才执行
	lazy val lastTemp : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

	//定义一个用于设置定时器时时间戳的状态
	lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]) )

	override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
		//每来一个数据从状态里去一个数据
		// 先取一个温度值
		val preTemp: Double = lastTemp.value()

		//当前定时器时间戳
		val curTimerTs: Long = currentTimer.value()

		//更新状态，更新温度值
		lastTemp.update(value.temperature)

		//温度连续上升且未设置过定时器：现在>之前，注册定时器
		if (value.temperature > preTemp && curTimerTs == 0) {
			//获取当前时间用于定时器时间戳
			val timerTimestamp: Long = ctx.timerService().currentProcessingTime() + 3000L
			//注册定时器
			ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
			currentTimer.update(timerTimestamp)
		} else if ( value.temperature < preTemp || preTemp == 0.0){
			//温度下降，或是来的数据时第一条；删除定时器
			ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
			//清空状态
			currentTimer.clear()
		}

	}

	//报警器触发，执行回调函数
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
		//输出报警信息
		out.collect(ctx.getCurrentKey + " 温度连续上升")
		currentTimer.clear()
	}
}
