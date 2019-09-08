package spark_rdd.itcast.cn.spark_rdd_pv_uv_topN.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object uv {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("uv")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val data: RDD[String] = sc.textFile("C:\\Users\\zero\\Desktop\\备课云10\\access.log")
    val map_rdd: RDD[Array[String]] = data.map(_.split(" "))
    val map1_rdd: RDD[String] = map_rdd.map(x=>x(0))
    val uv: RDD[(String, Int)] = map1_rdd.distinct().map(x=>("uv",1))
    val total_uv: RDD[(String, Int)] = uv.reduceByKey(_+_)
    total_uv.foreach(println)

  }
}
