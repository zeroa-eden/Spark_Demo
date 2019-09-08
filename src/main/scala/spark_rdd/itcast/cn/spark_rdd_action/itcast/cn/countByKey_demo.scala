package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 表示每个key对应的元素个数
  */
object countByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    println(rdd.countByKey().toBuffer)
  }
}
