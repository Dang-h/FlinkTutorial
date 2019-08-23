package apiTest.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import apiTest.source.{SensorReading, SensorSource}
import apiTest.utils.MyUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbsSink {
	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val inputDS1: DataStream[String] = env.readTextFile("F:\\workSpace\\FlinkTutorial\\file\\sensor.txt")
		val inputDS2: DataStream[SensorReading] = env.addSource(new SensorSource)

		env.setParallelism(1)


		val dataDS: DataStream[SensorReading] = MyUtils.dataOptSimple(inputDS1)

//		val dataDS: DataStream[SensorReading] = inputDS1.map(data => {
//			val dataArray: Array[String] = data.split(",")
//			SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
//		})

		dataDS.addSink(new MyJdbcSink)
		inputDS2.addSink(new MyJdbcSink)

		env.execute("Jdbc sink Test")

	}


}


class MyJdbcSink() extends RichSinkFunction[SensorReading] {

	//定义连接
	var con: Connection = _
	var insertStmt: PreparedStatement = _
	var updateStmt: PreparedStatement = _

	//创建连接
	override def open(parameters: Configuration): Unit = {
		super.open(parameters)

		con = DriverManager.getConnection("jdbc:mysql://sql:3306/test", "root", "mysql")

		//创建PreparedStatement,写sql
		insertStmt = con.prepareStatement("insert into temperature (sensor, temp) value (?, ?)")
		updateStmt = con.prepareStatement("update temperature set temp = ? where sensor = ?")
	}

	//对数据执行sql
	override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
		//更新数据
		updateStmt.setDouble(1, value.temperature)
		updateStmt.setString(2, value.id)
		updateStmt.execute()

		//如果没有数据更新，就执行插入
		if (updateStmt.getUpdateCount == 0) {
			insertStmt.setString(1, value.id)
			insertStmt.setDouble(2, value.temperature)
			insertStmt.execute()
		}
	}

	//关闭连接
	override def close(): Unit = {
		insertStmt.close()
		updateStmt.close()
		con.close()
	}
}
