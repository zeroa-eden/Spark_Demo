package sparksql.itcast.cn

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import sparksql.itcast.cn.sparkSSql_rdd_fanshe.Person

object spark2Mysql {
  def main(args: Array[String]): Unit = {

      //获取sparkSession
      val sparkSession: SparkSession = SparkSession.builder().appName("spark2Mysql").master("local[2]").getOrCreate()
      //通过sparkSession得到sparkContext
      val sparkContext: SparkContext = sparkSession.sparkContext
      //通过sparkContext 读取文本文件内容，得到RDD
      val arrRDD: RDD[Array[String]] = sparkContext.textFile("file:///C:\\Users\\zero\\Desktop\\备课云10\\person.txt").map(x => x.split(" "))
      //通过RDD，配合样例类，将我们的数据转换成样例类对象
      val personRDD: RDD[Person] = arrRDD.map(x => Person(x(0).toInt,x(1),x(2).toInt))
      //导入sparkSession当中的隐式转换，将我们的样例类对象转换成DataFrame
      import sparkSession.implicits._
      val personDF: DataFrame = personRDD.toDF()
      //打印dataFrame当中的数据
      val personDFShow: Unit = personDF.show()
      //将DataFrame注册成为一张表模型
      val personView: Unit = personDF.createTempView("person_view")
      //获取表当中的数据
      val result: DataFrame = sparkSession.sql("select * from person_view")
      //获取mysql连接
      val url ="jdbc:mysql://localhost:3306/test"
      val tableName = "person"
      val properties = new Properties()
      properties.setProperty("user","root")
      properties.setProperty("password","123456")
      //将我们查询的结果写入到mysql当中去
      val jdbc: Unit = result.write.mode(SaveMode.Append).jdbc(url,tableName,properties)
      sparkContext.stop()
      sparkSession.close()
  }
}
