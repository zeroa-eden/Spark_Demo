package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object combineByKey_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val scores = Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val input = sc.parallelize(scores)
    val combine = input.combineByKey(
      (v)=>(v,1),//(88,1)(95,1)(91.1)
      //(88,1),88 => 88 ,1
      (acc:(Int,Int),v)=>(acc._1+v,acc._2+1),//(88,1)
      //(88,1),95,1=>88+95+91,1+1+1
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))//()
    println(combine.collect().toBuffer)
    val result = combine.map{ case (key,value) => (key,value._1/value._2.toDouble)}
    println(result.collect().toBuffer)
  }
}
