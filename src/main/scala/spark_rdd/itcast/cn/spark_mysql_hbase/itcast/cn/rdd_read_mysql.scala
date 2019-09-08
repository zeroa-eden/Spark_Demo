package spark_rdd.itcast.cn.spark_mysql_hbase.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object rdd_read_mysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf ().setMaster ("local[2]").setAppName ("JdbcApp")
    val sc = new SparkContext (sparkConf)
    val rdd1 = new org.apache.spark.rdd.JdbcRDD (
      sc,
      () => {
        Class.forName ("com.mysql.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection ("jdbc:mysql://172.16.0.70:3306/spark", "root", "123456")
      },
      "select * from rddtable where id >= ? and id <= ?;",
      1,
      10,
      1,
      r => (r.getInt(1), r.getString(2)))

    println (rdd1.count () )
    rdd1.foreach (println (_) )
    sc.stop ()
  }
}
