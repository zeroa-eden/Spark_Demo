package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object takeSample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(1 to 10)
    //fraction 从rdd中随机抽出50%的数据
    var takesample1 = rdd.takeSample(true,1)
    var takesample2 = rdd.takeSample(true,1)

    println(takesample1.toBuffer)

  }
}
