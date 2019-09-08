package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * subtract取差集
  */
object subtract_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //rdd1-rdd rdd减去与rdd2相同=剩余rdd的元素，本人简写方式 ： A-B=A_剩
    val rdd1 = sc.parallelize(1 to 5)//12345
    val rdd = sc.parallelize(3 to 8)//345678
    println(rdd1.subtract(rdd).collect().toBuffer)
  }
}
