package sparksql.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparksql.itcast.cn.sparkSSql_rdd_fanshe.Person

object sparkSql_textFile {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()
   val dataFrame: Dataset[String] = spark.read.textFile("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\person.txt")
    dataFrame.createOrReplaceTempView("t_data")
    spark.sql("select * from t_data").show()

  }
}
