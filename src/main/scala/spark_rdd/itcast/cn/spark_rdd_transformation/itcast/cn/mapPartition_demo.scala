package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartition_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))
    def partitionsFun(iter : Iterator[(String,String)]) : Iterator[String] = {
      var woman = List[String]()
      while (iter.hasNext){
        val next = iter.next()
        next match {
          case (_,"female") => woman = next._1 :: woman //：：像队列的头部添加数据
          case _ =>
        }
      }
      woman.iterator
    }
    val result: RDD[String] = rdd.mapPartitions(partitionsFun)
    println(result.collect().toBuffer)
  }
}
