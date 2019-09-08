package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sample样本函数
  * 1、withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重复
  * 2、fraction：期望样本的大小作为RDD大小的一部分，抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
  * 当withReplacement=false时：选择每个元素的概率;分数一定是[0,1] ；
  * 当withReplacement=true时：选择每个元素的期望次数; 分数必须大于等于0。
  * 3、seed：计算随机数的种子根据这个seed随机抽取，一般情况下只用前两个参数就可以，
  * 那么这个参数是干嘛的呢，
  * 这个参数一般用于调试，有时候不知道是程序出问题还是数据出了问题，就可以将这个参数设置为定值
 **
 不会那么精准
  *抽取那片数据和给的随机数种子有关系
  */
object sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(1 to 10)
    //fraction 从rdd中随机抽出50%的数据
    var sample1 = rdd.sample(true,0.5,3)
    var sample2 = rdd.sample(false,0.5,3)
    println(sample1.collect().toBuffer)
    println(sample2.collect().toBuffer)

  }
}
