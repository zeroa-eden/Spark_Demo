package sparksql.itcast.cn

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
case class Small(line:String)
object sparkFunction {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkFunction").master("local[2]").getOrCreate()
    //通过sparkSession得到sparkContext
    val sparkContext: SparkContext = sparkSession.sparkContext
    //读取文件内容，获取RDD
    val fileRdd: RDD[String] = sparkContext.textFile("C:\\Users\\zero\\IdeaProjects\\Spark_Demo\\data\\udf.txt")
    //配合我们的样例类，将我们的每一行文件内容转换成一个样例类
    val smallRdd: RDD[Small] = fileRdd.map( x => Small(x))
    //导入spark的隐式转换
    import sparkSession.implicits._
    //将我们的RDD转换成DataFrame
    val smallDF: DataFrame = smallRdd.toDF()
    //df注册成为一张临时表
    smallDF.createOrReplaceTempView("small_table")
    //通过sparkSession进行UDF的注册，将我们的小写转换成大写
    sparkSession.udf.register("smallToBigger", new UDF1[String,String]() {
      @throws[Exception]
      override def call(t1: String): String = {
        t1.toUpperCase()
      }
    }, DataTypes.StringType)
    //使用UDF函数
    sparkSession.sql("select line, smallToBigger(line) as biggerLine from small_table").show()
    sparkSession.stop()


  }
}
