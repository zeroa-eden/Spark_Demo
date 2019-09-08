package spark_rdd.itcast.cn.spark_mysql_hbase.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object rdd_write_mysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(List("Female", "Male","Female"))

    data.foreachPartition(insertData)
  }

  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into rddtable1(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }
}
