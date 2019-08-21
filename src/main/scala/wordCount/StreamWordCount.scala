package wordCount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object StreamWordCount {

	def main(args: Array[String]): Unit = {
		//Source
		// 从外部命令获取参数
		val params: ParameterTool = ParameterTool.fromArgs(args)
		val host: String = params.get("host")
		val port: Int = params.getInt("port")

		//Transformation
		// 创建流处理环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.disableOperatorChaining()

		//接收socket文本流
		val textDstream: DataStream[String] = env.socketTextStream(host,port)

		//flatmap 和map处理数据
		//keyBy操作要根据Hash值重分区
		val wordCountDS: DataStream[(String, Int)] = textDstream.flatMap(_.split("\\s"))
		  .filter(_.nonEmpty)
		  .map((_, 1))
		  .keyBy(0)
		  .sum(1)

		//Sink
		// 打印数据
		wordCountDS.print()

		//启动executor执行任务
		env.execute("Socket stream wordCount")
	}

}
