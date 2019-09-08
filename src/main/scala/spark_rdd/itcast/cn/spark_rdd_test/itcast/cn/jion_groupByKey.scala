package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object jion_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    //求join
    val rdd3 = rdd1.join(rdd2)
    val rdd4: RDD[(String, Int)] = rdd1.union(rdd2)
    val rdd5: RDD[(String, Iterable[Int])] = rdd4.groupByKey()
    println(rdd5.collect().toBuffer)
  }
}
