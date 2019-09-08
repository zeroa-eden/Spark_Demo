package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mappartitinWithIndex
  * 拿到所有是女生的名字，并且在前面加上分区索引
  */
object mapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))
    def partitionsFun(index : Int, iter : Iterator[(String,String)]) : Iterator[String] = {
      var woman = List[String]()
      while (iter.hasNext){
        val next = iter.next()
        next match {
          case (_,"female") => woman = "["+index+"]"+next._1 :: woman
          case _ =>
        }
      }
      woman.iterator
    }

     val result = rdd.mapPartitionsWithIndex(partitionsFun)
    println(result.partitions.size)
    println(result.collect().toBuffer)
  }
}
