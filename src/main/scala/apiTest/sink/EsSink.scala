package apiTest.sink

import java.util

import apiTest.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSink {

	def main(args: Array[String]): Unit = {

		//配置Es
		val httpHosts = new util.ArrayList[HttpHost]()
		httpHosts.add(new HttpHost("sql", 9200))

		//创建ES的builder
		val esSink: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](
			httpHosts,
			new ElasticsearchSinkFunction[SensorReading] {
				//数据写入过程
				override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
					//要写入的数据，可以将数据写成一个JSON
					//数据存入map
					val dataMap = new util.HashMap[String, String]()
					dataMap.put("sensor_id", t.id)
					dataMap.put("timestamp", t.timestamp.toString)
					dataMap.put("temperature", t.temperature.toString)

					//创建index
					val indexRequest: IndexRequest = Requests.indexRequest().index("sensor").`type`("data").source(dataMap)

					//发送http请求
					requestIndexer.add(indexRequest)
					println("save data " + t + "successfully")
				}
			}
		)

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
		dataDS.addSink(esSink.build())

		env.execute("EsSink Test")

	}
}
