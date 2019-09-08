package sparksql.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object sparksql_parquet {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("json").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    sc.setLogLevel("WARN")
    val dataFrame: DataFrame = spark.read.json("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\people.json")
    dataFrame.createOrReplaceTempView("par_sql")
    spark.sql("select * from par_sql").show()
  }
}
