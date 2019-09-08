package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object repartition_coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(1 to 10,3)
    //利用repartition改变rdd1分区数
    //减少分区
    rdd1.repartition(2).partitions.size
    //增加分区
    rdd1.repartition(4).partitions.size
    //利用coalesce改变rdd1分区数
    //减少分区
    rdd1.coalesce(2).partitions.size
  }
}
