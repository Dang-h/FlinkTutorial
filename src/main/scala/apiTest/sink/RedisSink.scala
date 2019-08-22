package apiTest.sink

import apiTest.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {

	def main(args: Array[String]): Unit = {
		//配置redis连接
		//没有（）的函数调用一定不能加（）
		val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("sql").setPort(6379).build()

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		env.setParallelism(1)

		val inputDS: DataStream[String] = env.readTextFile("F:\\workSpace\\FlinkTutorial\\file\\sensor.txt")
		//数据处理
		val dataDS: DataStream[SensorReading] = inputDS.map(data => {
			val dataArray: Array[String] = data.split(",")
			SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
		})

		//sink
		dataDS.addSink(new RedisSink[SensorReading](config, new MyRedisSink()))
		dataDS.print()

		env.execute("RedisSinkTest")
	}


}


class MyRedisSink() extends RedisMapper[SensorReading] {

	// 保存到redis的命令描述,以Hset数据类型存入
	// HSET、key、value
	override def getCommandDescription: RedisCommandDescription = {
		new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
	}

	override def getKeyFromData(data: SensorReading): String = data.id

	override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
