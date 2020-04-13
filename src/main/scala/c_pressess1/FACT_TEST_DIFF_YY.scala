package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_DIFF_YY {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql(
      """
        |drop table if exists clinical_path.tmp_FACT_TEST_DIFF_YY
      """.stripMargin)
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_TEST_DIFF_YY
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.tmp_FACT_TEST_DIFF_YY as
        |select yydm,count(1) as cnt from
        |(select * from clinical_path.fact_test_zyjl where  floor(zyts)+1 <= 7
        | and floor(zyts)+1>=2) t group by yydm
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_DIFF_YY as
        |select t1.*,t2.hos_class, row_number() over(partition by t2.hos_class order by t1.cnt) as rank,
        |round(row_number() over(partition by t2.hos_class order by t1.cnt)/count(*) over(partition by t2.hos_class),4) as ratio
        |from clinical_path.tmp_FACT_TEST_DIFF_YY t1
        |left join  clinical_path.dic_dic_yljg t2
        |on t1.yydm=t2.hospital_id
      """.stripMargin)
  }
}
