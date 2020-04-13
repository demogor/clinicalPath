package c_pressess1

import org.apache.spark.sql.SparkSession

object DIC_DIC_MXFL1 {
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
        |drop table if exists clinical_path.DIC_DIC_MXFL1
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.DIC_DIC_MXFL1 as
        |select distinct sjmlmc,sdmlbz from clinical_path.DIC_DIC_MXFL
      """.stripMargin)
  }
}
