package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object fold_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var rdd1 = sc.makeRDD(1 to 4,2)
    val fold: Int = rdd1.aggregate(1)(
      //1,1to4=>1+1+1+2+3+4
      { (x: Int, y: Int) => x + y },
      //12 ,1 =>13
      { (a: Int, b: Int) => a + b }
    )
    println(rdd1.fold(1)(_+_))
  }
}
