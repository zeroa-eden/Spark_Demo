package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
    val group = wordPairsRDD.groupByKey()
    val group2: RDD[(String, Int)] = group.map(t => (t._1, t._2.sum))
    val map = group.map(t => (t._1, t._2.sum))
    println(map.collect().toBuffer)
    println(group.collect().toBuffer)

  }
}
