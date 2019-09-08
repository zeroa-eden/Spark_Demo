package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求并集
  */
object union_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(5 to 10)
    val rdd3 = rdd1.union(rdd2)
    println(rdd3.collect().toBuffer)
  }
}
