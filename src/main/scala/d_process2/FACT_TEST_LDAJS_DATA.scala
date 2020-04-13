package d_process2

import org.apache.spark.sql.SparkSession


object FACT_TEST_LDAJS_DATA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //明细表
    val fact_dic_mxxm = spark.sql(
      """
        |select * from clinical_path.fact_dic_mxxm
      """.stripMargin)
    //记录表
    val fact_test_zyjl = spark.sql(
      """
        |select * from clinical_path.fact_test_zyjl where floor(zyts)+1 <= 13
      """.stripMargin)
    //提供项目字典表
    val dic_dic_mxfl_3 = spark.sql(
      """
        |select * from clinical_path.dic_dic_mxfl_3
        |    where sjmlmc not in ('诊查费','护理费','诊察费和护理费','静脉注射液','床位费','输液器材','电解质溶液','输液器','注射')
      """.stripMargin)
    //医院字典表
    val DIC_DIC_YLJG = spark.sql(
      """
        |select * from clinical_path.DIC_DIC_YLJG
      """.stripMargin)

    val FACT_TEST_LDAJS_DATA = fact_dic_mxxm.as("t1").join(
      fact_test_zyjl.as("t2"), $"t1.lsh" === $"t2.lsh", "inner").join(
      dic_dic_mxfl_3.as("t3"), $"t1.mxxmbm" === $"t3.ybbm", "inner").join(
      DIC_DIC_YLJG.as("t4"), $"t1.jgdm" === $"t4.hospital_id", "inner").selectExpr(
      "t1.jgdm as yydm",
      "t4.name as yymc",
      "t4.hos_class as yydj",
      "t1.lsh as lsh",
      "t1.mxxmbm as ybbm",
      "t3.sjmlmc as sjmlmc",
      "t2.jsqsr as ryrq",
      "t2.jyr as cyrq",
      "floor(t2.zyts)+1 as zyts",
      "t1.mxxmsysj as xmsysj",
      "datediff(to_date(from_unixtime(unix_timestamp(t1.mxxmsysj,'yyyymmdd'),'yyyy-mm-dd')),to_date(t2.jsqsr)) + 1 as djtsy",
      "t1.mxxmsl as xmsl"
    )
    FACT_TEST_LDAJS_DATA.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_LDAJS_DATA")
  }
}
