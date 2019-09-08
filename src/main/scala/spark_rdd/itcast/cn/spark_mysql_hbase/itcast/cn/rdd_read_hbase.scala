package spark_rdd.itcast.cn.spark_mysql_hbase.itcast.cn

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object rdd_read_hbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "linux1")
    //HBase中的表名
    conf.set(TableInputFormat.INPUT_TABLE, "myuser")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println("hBaseRDD RDD Count:"+ count)
    hBaseRDD.cache()
    hBaseRDD.foreach {
      case (_, result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
        val color = Bytes.toString(result.getValue("info".getBytes, "color".getBytes))
        println("Row key:" + key + " Name:" + name + " Color:" + color)
    }
    sc.stop()
  }
}
