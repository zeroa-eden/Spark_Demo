package spark_hbase.itcast.cn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{RowFactory, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ddd {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //定义一个表结构,要和待加载的csv内容逐列对应上
    val schema = StructType(List(
      StructField("time", StringType, nullable = false),
      StructField("shihsishagnbaoxinxi", StringType, nullable = false),
      StructField("car_statu", StringType, nullable = false),
      StructField("car_speed", StringType, nullable = false),
      StructField("Cumulative mileage", StringType, nullable = false),
      StructField("Location status", StringType, nullable = false),
      StructField("longitude", StringType, nullable = false),
      StructField("lat", StringType, nullable = false)
    ))

    //测试数据所在的本地路径
    val userDataPath = "C:\\Users\\zero\\Desktop\\test2.csv"

    //创建sparksession
    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("UserAnalysis")
      .enableHiveSupport() //启用hive
      .getOrCreate()

    //将csv读取成RDD[String]
    val lineRdd: RDD[String] = sparkSession.sparkContext.textFile(userDataPath)
    val rowRDD = lineRdd.map { x => {
      val split = x.split(",")
      // val splitx.split("\\|")   如果csv文件内容以竖线分隔，使用这种方式切分，注意需要转义
      RowFactory.create(split(0), split(1), split(2), split(3), split(4), split(5), split(6), split(7))
    }
    }
    /**
      * （二）：利用测试数据中，任意1个行程所产生的车速、累计里程数据，计算该辆汽车有几次紧急加速？
      * 说明：“紧急加速”指20秒以内（含），车速增加一倍。例如：一辆车，20秒内，从10KM/小时增长到20KM/小时，即为一次急加速。
      * 程序语言编写要求：Python、Sclala、Spark任选
      */
    //调用SparkSession的方法把RDD[Row]转换成DataFrame
    val userDF = sparkSession.createDataFrame(rowRDD, schema)


    //显示DataFrame的前10行数据
    userDF.show(10)

    //将DataFrame注册成视图,然后即可使用hql访问
    userDF.createOrReplaceTempView("userDF")
    // where time   BETWEEN  '2018/12/13 9:23:07' AND  '2018/12/13 9:30:07'
    //执行hql语句,生成一个新DataFrame
    val value = "2018-12-13 00:00:06"
    val provinceDF = sparkSession.sql("select time from userDF where time = "+"\'"+value+"\'")

    //显示DataFrame的前十行数据
    provinceDF.show()
  }
}
