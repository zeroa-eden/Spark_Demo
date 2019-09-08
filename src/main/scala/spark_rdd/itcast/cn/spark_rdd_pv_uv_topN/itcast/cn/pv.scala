package spark_rdd.itcast.cn.spark_rdd_pv_uv_topN.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object pv {
  def main(args: Array[String]): Unit = {

    //todo：创建sparkconf，设置appName

    //todo:setMaster("local[2]")在本地模拟spark运行 这里的数字表示 使用2个线程

    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //todo:创建SparkContext

    val sc: SparkContext = new SparkContext(sparkConf)

    //todo:读取数据

    val file: RDD[String] = sc.textFile("C:\\Users\\zero\\Desktop\\备课云10\\access.log")

    //todo:将一行数据作为输入，输出("pv",1)

    val pvAndOne: RDD[(String, Int)] = file.map(x=>("pv",1))

    //todo:聚合输出

    val totalPV: RDD[(String, Int)] = pvAndOne.reduceByKey(_+_)

    totalPV.foreach(println)

    sc.stop()

  }
}
