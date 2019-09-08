package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)]
  * foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
  * foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)]
  */
object foldByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val para_fold: RDD[(String, List[(Int, Int)])] = rdd.mapPartitionsWithIndex {
      (paraId, iter) => {
        val para_map: mutable.Map[String, List[(Int, Int)]] = scala.collection.mutable.Map[String, List[(Int, Int)]]()
        var para_name = "para_" + paraId
        para_map(para_name) = List[(Int, Int)]()
        while (iter.hasNext) {
          para_map(para_name) :+= iter.next()
        }
        para_map.iterator
      }
    }
    println(para_fold.collect().toBuffer)
    val agg = rdd.foldByKey(0)(_+_)
    println(agg.collect().toBuffer)
  }
}
