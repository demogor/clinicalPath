package h_state

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FACT_TEST_GBJD_YY_RES {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //提供平均分数
    val FACT_TEST_GBJD_RES = spark.sql("select * from clinical_path.FACT_TEST_GBJD_RES")

    // 整理出每家医院的手术人数，匹配度均值
    val FACT_TEST_GBJD_YY_RES1 = FACT_TEST_GBJD_RES.filter("ssr>0").
      groupBy("yydm").agg(count("lsh") as "cnt", avg("total") as "avgTotal").select(
      col("yydm").as("yydm"),
      col("cnt").as("cnt"),
      round(col("avgTotal"), 3).as("avgTotal")
    )

    val FACT_TEST_GBJD_YY_RES2 = FACT_TEST_GBJD_RES.groupBy("yydm", "yymc", "yydj").agg(count("lsh") as "cnt").select(
      col("yydm").as("yydm"),
      col("yymc").as("yymc"),
      col("yydj").as("yydj"),
      col("cnt").as("cnt")
    ).as("t1").join(
      FACT_TEST_GBJD_YY_RES1.as("t2"), $"t1.yydm" === $"t2.yydm", "left").selectExpr(
      "t1.yydm as yydm", //医院代码
      "t1.yymc as yymc", //医院名称
      "t1.yydj as yydj", //医院等级
      "t1.cnt as zrc", //医院总人数
      "t1.cnt-nvl(t2.cnt,0) as fssrc", //非手术人数
      "round(nvl(t2.cnt,0)/t1.cnt,3) as ssrczb", //手术人数占比
      "nvl(t2.avgTotal,0) as avgTotal" //总分值
    )
    FACT_TEST_GBJD_YY_RES2.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_GBJD_YY_RES")
  }
}
