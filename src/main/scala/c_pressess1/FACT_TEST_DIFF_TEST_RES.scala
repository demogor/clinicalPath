package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_DIFF_TEST_RES {
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
        |drop table if exists clinical_path.fact_test_diff_test_res1
      """.stripMargin)
    spark.sql(
      """
        |create table  clinical_path.fact_test_diff_test_res1 as select hos_class,name ,t1.* from clinical_path.fact_test_diff_test_res0 t1
        |left join clinical_path.DIC_DIC_YLJG t2
        |on t1.yydm=t2.hospital_id
      """.stripMargin)
  }
}
