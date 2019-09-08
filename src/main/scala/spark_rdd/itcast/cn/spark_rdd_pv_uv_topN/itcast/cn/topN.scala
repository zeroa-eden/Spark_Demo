package spark_rdd.itcast.cn.spark_rdd_pv_uv_topN.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object topN {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("topn")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//    val data: RDD[String] = sc.textFile("C:\\Users\\zero\\Desktop\\备课云10\\access.log")
    val data: RDD[String] = sc.textFile(args(0))
    val map_rdd: RDD[Array[String]] = data.map(_.split(" "))
    //14619
    val filter_map: RDD[Array[String]] = map_rdd.filter(_.length>10)
//    println(filter_map.count())//13771
    val url_one: RDD[(String, Int)] = filter_map.map(x=>(x(10),1))

    val total_url: RDD[(String, Int)] = url_one.reduceByKey(_+_)
    val result: RDD[(String, Int)] = total_url.sortBy(_._2,false)
    val topN: Array[(String, Int)] = result.take(5)
    val topn_filter: Array[(String, Int)] = topN.filter(_._1 !=("\"-\""))
//    topn_filter.foreach(println)


  }
}
