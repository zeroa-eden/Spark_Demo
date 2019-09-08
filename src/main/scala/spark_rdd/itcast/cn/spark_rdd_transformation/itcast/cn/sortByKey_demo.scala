package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object sortByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
   println(rdd.sortByKey(true).collect().toBuffer)
   println(rdd.sortByKey(false).collect().toBuffer)
  }
}
