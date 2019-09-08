package sparksql.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object sparkSql_jsonDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json_data").setMaster("local")
      .setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val dataSourceDF: DataFrame =
      spark.read.json("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\students.json")
  dataSourceDF.createOrReplaceTempView("json_data")
    val goodStudent: DataFrame = spark.sql("select name ,score from json_data where score >= 80")
    val goodStudentNmae: Array[Any] = goodStudent.rdd.map(
      row => row(0)
    ).collect()
    println(goodStudentNmae.toBuffer)
    val studentInfoJSONs = Array("{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}")
    val data: RDD[String] = sc.parallelize(studentInfoJSONs,3)
    val studntData: DataFrame = spark.read.json(data)
    studntData.createOrReplaceTempView("test_data")
    var sql = "select name,age from test_data where name in ("
    /*for(i <- 0 until goodStudent.length) {
      sql += "'" + goodStudent(i) + "'"
      if(i < goodStudent.length - 1) {
        sql += ","
      }
    }
    sql += ")"
    spark.sql()*/
  }
}
