package c_pressess1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FACT_TEST_SSR_DATA0 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    val FACT_TEST_LDAJS_DATA=spark.sql("select * from clinical_path.FACT_TEST_LDAJS_DATA")
    FACT_TEST_LDAJS_DATA


  }
}
