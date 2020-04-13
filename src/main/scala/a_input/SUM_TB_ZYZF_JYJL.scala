package a_input

import org.apache.spark.sql.SparkSession

object SUM_TB_ZYZF_JYJL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      //.master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("Price Adjustment")
      .enableHiveSupport()
      .getOrCreate()
  }
}
