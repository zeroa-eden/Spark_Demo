package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object distinct_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val distinctRdd = sc.parallelize(List(1,2,1,5,2,9,6,1))
    val unionRDD = distinctRdd.distinct()
    val unionRDD1 = distinctRdd.distinct(2)

    println(unionRDD.collect().toBuffer)
    println(unionRDD1.collect().toBuffer)

  }
}
