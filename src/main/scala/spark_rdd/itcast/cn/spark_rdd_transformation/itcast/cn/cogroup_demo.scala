package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 元组中的key进行聚合
  */
object cogroup_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))
    println(rdd.cogroup(rdd1).collect().toBuffer)
    val rdd2 = sc.parallelize(Array((4, 4), (2, 5), (3, 6)))
    println(rdd.cogroup(rdd2).collect().toBuffer)
    val rdd3 = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    println(rdd3.cogroup(rdd2).collect().toBuffer)
  }
}
