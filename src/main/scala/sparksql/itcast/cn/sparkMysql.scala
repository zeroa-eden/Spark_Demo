package sparksql.itcast.cn

import java.util.Properties

import org.apache.spark.sql.SparkSession

object sparkMysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder().appName("sparkMysql").master("local").getOrCreate()
    val url = "jdbc:mysql://localhost:3306/test"
    val tableName = "emp"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    val tableDatas: Unit = spark.read.jdbc(url,tableName,properties).show()
    spark.close()
  }
}
