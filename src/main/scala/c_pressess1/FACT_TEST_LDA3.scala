package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_LDA3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_lda_df0 = spark.sql("select lsh,djtsy,sjmlmc from clinical_path.fact_test_lda_data_2 limit 10")

  }
}
