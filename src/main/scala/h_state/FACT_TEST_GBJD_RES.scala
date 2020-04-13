package h_state

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FACT_TEST_GBJD_RES {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val FACT_TEST_SS_RES=spark.sql(
      """
        |select t.*,nvl(t1.simi,0) as ssqsimi,nvl(t2.simi,0) as sshsimi
        |from clinical_path.FACT_TEST_SSR_DATA t
        |left join clinical_path.fact_test_ssq_simi t1
        |on t.lsh=t1.lsh
        |left join clinical_path.fact_test_ssh_simi t2
        |on t.lsh=t2.lsh
      """.stripMargin)

    val FACT_TEST_SS_RES0 = FACT_TEST_SS_RES.filter("ssr<>0").cache()
    val maxSsq0=FACT_TEST_SS_RES0.select(max($"ssqsimi")).collect.map(row=>row.getDouble(0))
    val maxSsq=maxSsq0(0)
    val minSsq0=FACT_TEST_SS_RES0.select(min($"ssqsimi")).collect.map(row=>row.getDouble(0))
    val minSsq=minSsq0(0)
    val maxSsh0=FACT_TEST_SS_RES0.select(max($"sshsimi")).collect.map(row=>row.getDouble(0))
    val maxSsh=maxSsh0(0)
    val minSsh0=FACT_TEST_SS_RES0.select(min($"sshsimi")).collect.map(row=>row.getDouble(0))
    val minSsh=minSsh0(0)

    val FACT_TEST_SS_RES1= FACT_TEST_SS_RES0.
      withColumn("zytsZyr", when($"zyts" <= 5, 1).otherwise(round((-($"zyts" - 5) / $"zyts" + 1),3)) * 0.5).
      withColumn("zytsSsr",when($"ssr"<=3,1).otherwise(round((-($"zyts"-3)/$"zyts"+1),3))*0.5).
      withColumn("ssqSimiS",round(($"ssqsimi"-minSsq)/(maxSsq-minSsq),3)).
      withColumn("sshSimiS",round(($"sshsimi"-minSsh)/(maxSsq-minSsh),3)).
      withColumn("Total",round((($"zytsZyr"+$"zytsSsr")*0.4+$"ssqSimiS"*0.3+$"sshSimiS"*0.3),3))

    val FACT_TEST_GBJD_YS_RES=FACT_TEST_SS_RES.as("t1").
      join(FACT_TEST_SS_RES1.as("t2"),$"t1.lsh"===$"t2.lsh","left").selectExpr(
      "t1.yydm as yydm",
      "t1.yymc as yymc",
      "t1.yydj as yydj",
      "t1.lsh as lsh",
      "t1.zyts as zyts",
      "t1.ssr as ssr",
      "t1.xmset as xmset",
      "t1.ssqset as ssqset",
      "t1.sshset as sshset",
      "t1.ssqsimi as ssqsimi",
      "t1.sshsimi as sshsimi",
      "nvl(t2.zytszyr,0) as zytszyr",
      "nvl(t2.zytsssr,0) as zytsssr",
      "nvl(t2.ssqsimis,0) as ssqsimis",
      "nvl(t2.sshsimis,0) as sshsimis",
      "nvl(t2.total,0) as total"
    )
    //保存数据到hive中
    FACT_TEST_GBJD_YS_RES.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_GBJD_RES")
  }
}