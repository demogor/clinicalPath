package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_DIFF_DATA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_TEST_DIFF_DATA
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_DIFF_DATA as
        |select t1.lsh as lsh,
        |t1.djtsy as djtsy,
        |t1.topic as clinical_path,
        |t1.zyts as zyts,
        |t2.yydm as yydm,
        |t3.name as yymc,
        |t3.hos_class as yydj
        |from clinical_path.fact_test_lda2_res2 t1
        |inner join (select * from fact_test_lda2_res2_0 where ratio>0.2 and cnt<>1) t4
        |on concat(t1.topic,'*',t1.zyts)=t4.clinicalpath
        |inner join clinical_path.FACT_DIC_ZYJS t2 on
        |t1.lsh=t2.lsh
        |inner join clinical_path.DIC_DIC_YLJG t3
        |on t3.hospital_id=t2.yydm
      """.stripMargin)
    //之后更新为
  }
}
