package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object aggreagetByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(List((1,3),(1,4),(1,5),(1,6),(1,4),(2,3),(3,6),(3,8)),5)
    val agg = rdd.aggregateByKey(10)(_+_,_+_)
    //math.max(_,_)是求每个分区中每个key的最大值
    val paraIndex: RDD[(String, List[(Int, Int)])] = rdd.mapPartitionsWithIndex {
      (partid, iter) => {
        // var part_map = scala.collection.mutable.Map[String,List[Int,Int]]()
        val part_map: mutable.Map[String, List[(Int, Int)]] = scala.collection.mutable.Map[String, List[(Int, Int)]]()
        var part_name = "part_" + partid
        part_map(part_name) = List[(Int, Int)]()
        while (iter.hasNext) {
          part_map(part_name) :+= iter.next() //:+= 列表尾部追加元素
        }
        part_map.iterator
      }
    }
   println(paraIndex.collect().toBuffer)
//ArrayBuffer((part_0,List((1,3), (1,2))), (part_1,List((1,4), (2,3))), (part_2,List((3,6), (3,8))))
    //求每个分区中key的最大值
    //part_0,List(1,3))，part_1,List((1,4), (2,3))，part_2,List(((3,8))
    val rdd1 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),1)


    val paraIndex1: RDD[(String, List[(Int, Int)])] = rdd1.mapPartitionsWithIndex {
      (partid, iter) => {
        // var part_map = scala.collection.mutable.Map[String,List[Int,Int]]()
        val part_map: mutable.Map[String, List[(Int, Int)]] = scala.collection.mutable.Map[String, List[(Int, Int)]]()
        var part_name = "part_" + partid
        part_map(part_name) = List[(Int, Int)]()
        while (iter.hasNext) {
          part_map(part_name) :+= iter.next() //:+= 列表尾部追加元素
        }
        part_map.iterator
      }
    }
    val agg1: Array[(Int, Int)] = rdd1.aggregateByKey(10)(math.max(_,_),_+_).collect()
    println(paraIndex1.collect().toBuffer+"huahua")
    println(agg.collect().toBuffer)
    //ArrayBuffer((3,8), (1,7), (2,3))
    println(agg1.toBuffer)
  }
}
