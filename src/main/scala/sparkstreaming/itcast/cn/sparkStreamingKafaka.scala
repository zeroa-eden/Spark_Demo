//package sparkstreaming.itcast.cn
//
//
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
////import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext, TaskContext}
//
//object sparkStreamingKafaka {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
//    val context = new SparkContext(conf)
//    context.setLogLevel("WARN")
//    val ssc = new StreamingContext(context, Seconds(1))
//    //创建topic
//    val brobrokers = "linux1:9092,linux2:9092,linux3:9092"
//    val sourcetopic="sparkafka";
//    //创建消费者组
//    var group="sparkafkaGroup"
//    //消费者配置
//    val kafkaParam = Map(
//      "bootstrap.servers" -> brobrokers,//用于初始化链接到集群的地址
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      //用于标识这个消费者属于哪个消费团体
//      "group.id" -> group,
//      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
//      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
//      "auto.offset.reset" -> "latest",
//      //如果是true，则这个消费者的偏移量会在后台自动提交
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    );
//    var stream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String,String](Array("sparkafka"),kafkaParam))
//
//
//    //循环遍历每个RDD当中的数据
//    stream.foreachRDD(f =>{
//      //判断如果rdd当中有数据，就进行处理，没有数据就不用处理
//      if(f.count() > 0){
//        println("接收kafka当中的数据")
//        //每个分区当中的数据进行循环遍历，遍历每个分区当中每一行的数据
//        f.foreach(f =>{
//          //获取kafka当中的数据内容
//          val kafkaValue: String = f.value()
//          println(kafkaValue)
//        })
//        //打印offset的信息
//        val offsetRanges: Array[OffsetRange] = f.asInstanceOf[HasOffsetRanges].offsetRanges
//        f.foreachPartition { iter =>
//          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//        }
//        println("=============================")
//        // 等输出操作完成后提交offset
//        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//      }
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
