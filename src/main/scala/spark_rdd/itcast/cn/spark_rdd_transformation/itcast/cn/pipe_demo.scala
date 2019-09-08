package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * pipe通过shell命令来对RDD个分区进行管道化，通过pipe变换将一些shell命令用于spark中生成的新RDD
  */
object pipe_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    println(rdd.pipe("head -n 1").collect().toBuffer)

  }
}
