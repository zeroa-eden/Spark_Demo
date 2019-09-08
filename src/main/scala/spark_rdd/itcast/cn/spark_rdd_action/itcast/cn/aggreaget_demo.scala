package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object aggreaget_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var rdd1 = sc.makeRDD(1 to 10,2)
   println(rdd1.reduce(_+_).toString)
    val agge: Int = rdd1.aggregate(1)(
      //1，1 to 10 =>1+1+1+1+2---+10
      { (x: Int, y: Int) => x + y },
      //57 +1
      { (a: Int, b: Int) => a + b }
      //等于58
    )
    println(agge.toString)
    val agge1: Int =
    rdd1.aggregate(1)(
      {(x : Int,y : Int) => x * y},
      {(a : Int,b : Int) => a + b})
    println(agge1.toString)

    val textRDD = sc.parallelize(List("A", "B", "C", "D", "D", "E"))
    val resultRDD = textRDD.aggregate((0, ""))(
      //((0, ""),List("A", "B", "C", "D", "D", "E"))=>
      //(1,""+:+""+:A)
      (acc, value)=>{(acc._1+1, acc._2+":"+value)},
      //(1,""+:+""+:A)，(1,""+:+""+:A)
      (acc1, acc2)=> {(acc1._1+acc2._1, acc1._2+":"+acc2._2)})
    //https://blog.csdn.net/liangyihuai/article/details/54377226 参考

   println(resultRDD.toString())
  }
}
