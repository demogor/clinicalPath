package d_process2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FACT_TEST_CLUSTER_DATA {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_zyjl = spark.sql(
      """
        |select yydm, lsh, zyts from clinical_path.fact_test_zyjl
        |where zdbm='N20.100' and zyts>0
      """.stripMargin)
    val fact_dic_mxxm = spark.sql(
      """
        |select lsh,
        | substr(mxxmbm, 1, 1) as fylb,
        | sum(mxxmybjsfy) as fy
        | from clinical_path.fact_dic_mxxm
        | where zdbm='N20.100'
        |group by lsh, substr(mxxmbm, 1, 1)
      """.stripMargin)

    val FACT_TEST_CLUSTER_DATA = fact_test_zyjl.as("t1").join(
      fact_dic_mxxm.as("t2"), $"t1.lsh" === $"t2.lsh", "left").selectExpr(
      "t1.lsh as lsh",
      "t1.zyts as zyts",
      "t2.fylb as fylb",
      "t2.fy as fy"
    ).groupBy("zyts", "fylb").agg(round(avg("fy"), 3) as "fyze")
    FACT_TEST_CLUSTER_DATA.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_CLUSTER_DATA")
  }
}
