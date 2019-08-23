package apiTest.utils

import apiTest.source.SensorReading
import org.apache.flink.streaming.api.scala._

object MyUtils{

	def dataOptSimple(inputData: DataStream[String]): DataStream[SensorReading] = {
		inputData.map((data: String) => {
			val dataArray: Array[String] = data.split(",")
			SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
		})
	}

}
