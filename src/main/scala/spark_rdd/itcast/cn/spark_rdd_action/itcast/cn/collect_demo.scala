package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object collect_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//    var rdd1 = sc.makeRDD(1 to 10,2)
//    println(rdd1.collect().toBuffer)
//    val seq = (lambda)

    println(sc.parallelize(List(1,2,3,4),2).aggregate(0,0))
  }
}
