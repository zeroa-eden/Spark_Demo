package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2),  ("shuke", 1)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 3), ("shuke", 2), ("kitty", 5)))
    val rdd3 = rdd1.union(rdd2)
    println(rdd3.collect().toBuffer)
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_+_)
    println(rdd4.collect().toBuffer)
    val rdd5: RDD[(Int, String)] = rdd3.map(t => (t._2,t._1)).sortByKey(false)
    println(rdd5.collect().toBuffer+"rdd5")
    val rdd6: RDD[(String, Int)] = rdd3.map(t => (t._2,t._1)).sortByKey(false).map(t => (t._2,t._1))
    println(rdd6.collect().toBuffer+"rdd6")

  }
}
