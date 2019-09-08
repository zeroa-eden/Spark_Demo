package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object reduce_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.makeRDD(1 to 10,2)
    println(rdd1.reduce(_+_))
    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    println( rdd2.reduce((x,y)=>(x._1 + y._1,x._2 + y._2)))
  }
}
