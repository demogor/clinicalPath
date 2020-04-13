package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_LDA_RES {
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
        |drop table if exists clinical_path.FACT_TEST_LDA_RES
      """.stripMargin)

    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA_RES as
        | select concat_ws('-',collect_list(cast(t.djtsy as string))) as djtsy
        | ,concat_ws('-',collect_list(cast(t.topic as string))) as topic from
        | (select *,3 as zyts from clinical_path.fact_test_lda_ladmodel order by djtsy ) t group by t.zyts;
      """.stripMargin)
  }
}
