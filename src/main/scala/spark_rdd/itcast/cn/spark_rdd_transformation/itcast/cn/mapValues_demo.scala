package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 争对key-value的类型，支队v进行操作
  */
object mapValues_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    println(rdd3.mapValues(_+"|||").collect().toBuffer)
  }
}
