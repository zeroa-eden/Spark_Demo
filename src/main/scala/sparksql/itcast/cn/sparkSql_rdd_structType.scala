package sparksql.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object sparkSql_rdd_structType {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("structType")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    sc.setLogLevel("WARN")
    val data: RDD[String] =
      sc.textFile("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\person.txt")
    val arrayRDD: RDD[Array[String]] = data.map(
      x => x.split(" ")
    )
    val struct: StructType = new StructType().add("id", IntegerType, true).
      add("name", StringType, true).add("age", IntegerType, true)
    val rowRDD: RDD[Row] = arrayRDD.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    val personDataFrame: DataFrame = spark.createDataFrame(rowRDD,struct)
    personDataFrame.printSchema()
    val personView: Unit = personDataFrame.createOrReplaceTempView("t_person_view")
    spark.sql("select * from t_person_view").show()
    sc.stop()
    spark.close()


  }
}
