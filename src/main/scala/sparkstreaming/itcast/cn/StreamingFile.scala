package sparkstreaming.itcast.cn

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingFile {
  def main(args: Array[String]): Unit = {
    //设置sparkConf配置
    val sparkConf: SparkConf = new SparkConf().setAppName("streamingFile").setMaster("local[2]")
    //通过sparkConf得到sparkContext
    val sparkContext = new SparkContext(sparkConf)
    //设置日志输出级别
    sparkContext.setLogLevel("WARN")
    //通过sparkContext得到streamingContext
    val streamingContext = new StreamingContext(sparkContext,Seconds(5))
    //设置sparkStreaming保存目录
    streamingContext.checkpoint("./hdfs-data")
    //读取hdfs某一个目录下的所有的文件
    val fileStream: DStream[String] = streamingContext.textFileStream("hdfs://192.168.192.134:8020/sparkfile")
//    val fileStream: DStream[String] = streamingContext.textFileStream("C:\\Users\\zero\\Desktop\\1")
    //文件内容按照空格进行切分
    val words: DStream[String] = fileStream.flatMap(x => x.split(" "))
    //每个单词记作为1
    val wordAndOne: DStream[(String, Int)] = words.map(x => (x,1))
    //更新每个单词的状态，传入一个我们自定义的updateFunction
    val key: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)
    key.print()
//key.saveAsTextFiles("C:\\Users\\zero\\Desktop\\1\\")
    streamingContext.start()

    streamingContext.awaitTermination()
  }
  def updateFunc(newValues:Seq[Int],runnintCount:Option[Int]):Option[Int]={
    val finalResult = runnintCount.getOrElse(0) + newValues.sum
    Option(finalResult)
  }
}
