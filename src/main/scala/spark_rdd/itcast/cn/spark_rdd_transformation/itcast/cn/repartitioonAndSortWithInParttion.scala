package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object repartitioonAndSortWithInParttion {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val array=Array(2,4,6,67,3,45,26,35,789,345)
    val data=sc.parallelize(array)
    // 替换repartition组合sortBy
    data.zipWithIndex().repartitionAndSortWithinPartitions(new HashPartitioner(1)).foreach(println)
  }
}
