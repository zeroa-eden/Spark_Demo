package spark_rdd.itcast.cn.spark_rdd_pv_uv_topN.itcast.cn

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("iplocation")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("C:\\Users\\zero\\Desktop\\备课云10\\ip.txt")
    val map: RDD[(String, String, String, String)] = data.map(x => x.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))
    val broadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(map.collect())
    val file2: RDD[String] =
      sc.textFile("C:\\Users\\zero\\Desktop\\备课云10\\20090121000132.394251.http.format")
    val ipRdd: RDD[String] = file2.map(x => x.split("\\|")(1))
    val partitions: RDD[((String, String), Int)] = ipRdd.mapPartitions(
      iter => {
        val value: Array[(String, String, String, String)] = broadcast.value
        iter.map(ip => {
          val longIp: Long = ipToLong(ip)
          val resultIndex: Int = binarySearch(longIp, value)
          ((value(resultIndex)._3, value(resultIndex)._4), 1)
        })
      }
    )
    val key: RDD[((String, String), Int)] = partitions.reduceByKey(_ + _)
    key.foreachPartition(data2Mysql)
    sc.stop()


  }

  def ipToLong(ip: String): Long = {
    val splits: Array[String] = ip.split("\\.")
    var returnNum: Long = 0
    for (num <- splits) {
      returnNum = num.toLong | returnNum << 8L
    }
    returnNum
  }

  def binarySearch(longIp: Long, value: Array[(String, String, String, String)]): Int = {
    var start = 0
    var end = value.length - 1
    while (start <= end) {
      var middle = (start + end) / 2
      if (longIp >= value(middle)._1.toLong && longIp <= value(middle)._2.toLong) {
        return middle
      }
      if (longIp < value(middle)._1.toLong) {
        end = middle - 1
      }
      if (longIp > value(middle)._2.toLong) {
        start = middle + 1
      }

    }
    -1
  }

  val data2Mysql = (result: Iterator[((String, String), Int)]) => {
    val connection: Connection =
      DriverManager.getConnection("jdbc:mysql://172.16.0.70:3306/spark", "root", "123456")
    val statement: PreparedStatement =
      connection.prepareStatement("insert into iplocation(longitude,latitude,total_count) value(?,?,?)")
 result.foreach(
   x => {
     statement.setString(1,x._1._1)
     statement.setString(2,x._1._2)
     statement.setInt(3,x._2)
     statement.execute()
   }
 )
statement.close()
    connection.close()
  }
}
