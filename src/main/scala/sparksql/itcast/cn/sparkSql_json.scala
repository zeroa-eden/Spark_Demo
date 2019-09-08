package sparksql.itcast.cn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object sparkSql_json {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("json").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    sc.setLogLevel("WARN")
    val dataFrame: DataFrame = spark.read.parquet("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\users.parquet")
    dataFrame.createOrReplaceTempView("par_sql")
    spark.sql("select * from par_sql").show()
  }
}
