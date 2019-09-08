package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object sortBy_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(List(1,2,3,4))
    val rdd1 = sc.parallelize(Array(("huhua",2),("xiaxia",4),("jiajia",0)))

    println(rdd.sortBy(x => x).collect().toBuffer)
    println(rdd.sortBy(x => x%3).collect().toBuffer)
    println(rdd1.sortBy(_._2,false).collect().toBuffer)
  }
}
