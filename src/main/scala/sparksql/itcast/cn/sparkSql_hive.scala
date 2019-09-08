package sparksql.itcast.cn

import org.apache.spark.sql.SparkSession

object sparkSql_hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("sparksql_hive")
      .config("spark.sql.warehouse.dir","f:\\spark-warehouse") //指定spark仓库地址
      .enableHiveSupport().getOrCreate()
   /* spark.sql("create table if not exists student3(id int,name String,age int) row format delimited fields terminated by ' '")
    spark.sql("load data local inpath './data/student.csv' overwrite into table student3")
    spark.sql("select * from student2").show()*/

    spark.sql(" show databases").show()
    //spark.sql(" create table default.huahua(id Int,name String,age int) row format delimited fields terminated by ' '").show()
    //spark.sql("load data local inpath './data/student.csv' overwrite into table huahua")
    spark.sql(" show tables").show()

    spark.stop()
  }
}
