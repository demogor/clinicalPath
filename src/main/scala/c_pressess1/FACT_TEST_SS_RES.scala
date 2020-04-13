package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_SS_RES {
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
        |drop table if exists clinical_path.FACT_TEST_SS_RES
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_SS_RES as
        |select t.*,nvl(t1.simi,0) as ssqsimi,nvl(t2.simi,0) as sshsimi
        |from clinical_path.FACT_TEST_SSR_DATA t
        |left join clinical_path.fact_test_ssq_simi t1
        |on t.lsh=t1.lsh
        |left join clinical_path.fact_test_ssh_simi t2
        |on t.lsh=t2.lsh
      """.stripMargin)
  }
}
