package sparkstreaming.itcast.cn

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingTCPWindow {
  def main(args: Array[String]): Unit = {
    //配置sparkConf参数
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingTCPWindow").setMaster("local[2]")
    //构建sparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //构建StreamingContext对象，每个批处理的时间间隔
    val scc: StreamingContext = new StreamingContext(sc,Seconds(2))
    //注册一个监听的IP地址和端口  用来收集数据
    val lines: ReceiverInputDStream[String] = scc.socketTextStream("192.168.192.134",9999)
    //切分每一行记录
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //每个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //reduceByKeyAndWindow函数参数意义：
    // windowDuration:表示window框住的时间长度，如本例5秒切分一次RDD，框10秒，就会保留最近2次切分的RDD
    //slideDuration:  表示window滑动的时间长度，即每隔多久执行本计算
//    val map = new Map[String]()
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(2),Seconds(6))
//    result.foreachRDD(
//        rdd=>{
////          map+=rdd.collectAsMap()
//        rdd.foreach(println)
//      }
//    )
    result.print()
    scc.start()
    scc.awaitTermination()
  }
}
