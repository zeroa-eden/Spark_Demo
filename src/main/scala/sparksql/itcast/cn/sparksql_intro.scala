package sparksql.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object sparksql_intro {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local").setAppName("sparksql")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("q",1),("s",2)))
    rdd.map{
      line => println("运行")
        line._1
    }
    val saveoptions = Map("header"->"true","delimiter"->"\t","path"->"hdfs://linux18020/test")

  }
}
