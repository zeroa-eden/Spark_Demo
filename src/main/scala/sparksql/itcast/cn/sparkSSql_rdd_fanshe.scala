package sparksql.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object sparkSSql_rdd_fanshe {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("fanmshe")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    sc.setLogLevel("WARN")
    val data: RDD[String] = sc.textFile("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\person.txt")
    val wordArray: RDD[Array[String]] = data.map(_.split(" "))
    val personRDD: RDD[Person] = wordArray.map(
      x => {
        Person(x(0).toInt, x(1), x(2).toInt)
      }
    )
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()
    personDF.printSchema()
    personDF.show()
    println(personDF.head())
    //查询name字段的所有值
    personDF.select("name").show()
    personDF.select($"name").show()
    personDF.select(personDF.col("name")).show()
    personDF.select(new Column("name")).show()
    //将年龄的值进行 + 1
    personDF.select($"id", $"name", $"age", $"age" + 1).show()
    //按照年龄进行分组
    personDF.groupBy($"age").count().show()

    /** *******************************sparkSql sql风格语法 开始 **************************************/
    println("**************************")

    val perrsonTable: Unit = personDF.registerTempTable("t_person")
    val personView: Unit = personDF.createOrReplaceTempView("t_person")
    spark.sql("select * from t_person").show
    spark.sql("select * from t_person where name = 'huahua'").show()
    sc.stop()
    spark.close()
  }

  case class Person(id: Int, name: String, age: Int)

}
