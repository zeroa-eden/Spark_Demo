package spark_wc.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //设置spark的配置文件信息
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //构建sparkcontext上下文对象，它是程序的入口,所有计算的源头
    val sc: SparkContext = new SparkContext(sparkConf)
    //读取文件
    val file: RDD[String] = sc.textFile("file:///C:\\Users\\zero\\Desktop\\kafka.txt")
    //对文件中每一行单词进行压平切分
    val words: RDD[String] = file.flatMap(_.split(" "))
    //对每一个单词计数为1 转化为(单词，1)
    val wordAndOne: RDD[(String, Int)] = words.map(x=>(x,1))
    //相同的单词进行汇总 前一个下划线表示累加数据，后一个下划线表示新数据
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //保存数据到HDFS
//    result.saveAsTextFile("file:///C:\\Users\\zero\\Desktop\\wordcount\\wordcount_out.txt")
    result.foreach(println)
//    Thread.sleep(1000000)
    sc.stop()
  }
}
