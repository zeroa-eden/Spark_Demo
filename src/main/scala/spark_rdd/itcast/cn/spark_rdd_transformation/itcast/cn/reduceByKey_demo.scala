package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
     val rdd: RDD[(String, Int)] = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
    val reduce = rdd.reduceByKey((x,y) => x+y)
    println(reduce.collect().toBuffer)

  }
}
