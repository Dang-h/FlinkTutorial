package wordCount

import org.apache.flink.api.scala._

object WordCount {
	def main(args: Array[String]): Unit = {
		//创建执行环境
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

		//从文件中读取
		val inputPath = "F:\\workSpace\\FlinkTutorial\\file\\input.txt"

		val inputDS: DataSet[String] = env.readTextFile(inputPath)

		//分词后进行分组，聚合
		val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

		//输出结果
		wordCountDS.print()
	}

}
