package c_pressess1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FACT_TEST_SSR_DATA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val FACT_TEST_LDAJS_DATA = spark.sql(
      """
        |SELECT
        |t.yydm,
        |t.yymc,
        |t.yydj,
        |t.lsh,
        |t.zyts,
        |t.djtsy,
        |substr(ybbm,2,9) as xmbm,
        |t.sjmlmc as xmmc
        |from clinical_path.FACT_TEST_LDAJS_DATA t
      """.stripMargin)
    //获取手术日
    val FACT_TEST_LDAJS_DATA0 = FACT_TEST_LDAJS_DATA.
      filter("xmbm in ('311000026','330100005')").
      groupBy("lsh", "djtsy").agg(countDistinct("xmbm") as "cnt").filter($"cnt" > 1).
      //去除多次手术的
      groupBy("lsh").agg(min("djtsy") as "djtsy").cache()
//数据预处理
    //手术前数据需要保存
    val FACT_TEST_SSQ_DATA0 = FACT_TEST_LDAJS_DATA.as("t1").
      join(FACT_TEST_LDAJS_DATA0.as("t2"), $"t1.lsh" === $"t2.lsh" && $"t1.djtsy" < $"t2.djtsy", "inner").selectExpr(
      "t1.yydm as yydm",
      "t1.yymc as yymc",
      "t1.yydj as yydj",
      "t1.lsh as lsh",
      "t1.zyts as zyts",
      "t1.djtsy as djtsy",
      "t1.xmbm as xmbm",
      "t1.xmmc as xmmc").cache()
    //手术后数据需要保存
    val FACT_TEST_SSH_DATA0 = FACT_TEST_LDAJS_DATA.as("t1").
      join(FACT_TEST_LDAJS_DATA0.as("t2"), $"t1.lsh" === $"t2.lsh" && $"t1.djtsy" > $"t2.djtsy", "inner").selectExpr(
      "t1.yydm as yydm",
      "t1.yymc as yymc",
      "t1.yydj as yydj",
      "t1.lsh as lsh",
      "t1.zyts as zyts",
      "t1.djtsy as djtsy",
      "t1.xmbm as xmbm",
      "t1.xmmc as xmmc").cache()

    //得到FACT_TEST_GBJB_XMTF结果
    val FACT_TEST_GBJB_XMTF_SSQ=FACT_TEST_SSQ_DATA0.groupBy(
      "yydm","yymc","yydj","lsh","xmmc").agg(count("xmbm") as "frequence").selectExpr(
      "yydm","yymc","yydj","lsh",
      "'2' as xmlb",
      "xmmc",
      "frequence",
      "row_number()over(partition by lsh order by frequence desc) as rank_fre"
    )
    val FACT_TEST_GBJB_XMTF_SSH=FACT_TEST_SSH_DATA0.groupBy("yydm","yymc","yydj","lsh","xmmc").agg(count("xmbm") as "frequence").selectExpr(
      "yydm","yymc","yydj","lsh",
      "'3' as xmlb",
      "xmmc","frequence",
    "row_number()over(partition by lsh order by frequence desc) as rank_fre")
    val FACT_TEST_GBJB_XMTF=FACT_TEST_LDAJS_DATA.groupBy(
      "yydm","yymc","yydj","lsh","xmmc").agg(count("xmbm") as "frequence").selectExpr(
      "yydm","yymc","yydj","lsh",
      "'1' as xmlb",
      "xmmc","frequence",
    "row_number()over(partition by lsh order by frequence desc) as rank_fre"
    ).union(FACT_TEST_GBJB_XMTF_SSQ).union(FACT_TEST_GBJB_XMTF_SSH)


    //手术前特征集
    val FACT_TEST_SSQ_DATA = FACT_TEST_SSQ_DATA0.
      groupBy("lsh").agg(concat_ws(",", collect_list("xmmc")) as "ssqset")
    //手术后特征集
    val FACT_TEST_SSH_DATA = FACT_TEST_SSH_DATA0.
      groupBy("lsh").agg(concat_ws(",", collect_list("xmmc")) as "sshset")

    //全部流水号的特征集
    val FACT_TEST_SSR_DATA = FACT_TEST_LDAJS_DATA.
      groupBy("yydm","yymc","yydj","lsh", "zyts").agg(concat_ws(",", collect_list("xmmc")) as "xmset").as("t1").
      join(FACT_TEST_LDAJS_DATA0.as("t2"), $"t1.lsh" === $"t2.lsh", "left").
      join(FACT_TEST_SSQ_DATA.as("t3"), $"t1.lsh" === $"t3.lsh", "left").
      join(FACT_TEST_SSH_DATA.as("t4"), $"t1.lsh" === $"t4.lsh", "left").
      selectExpr(
        "t1.yydm as yydm",
        "t1.yymc as yymc",
        "t1.yydj as yydj",
        "t1.lsh as lsh",
        "t1.zyts as zyts",
        //得到手术日
        "nvl(t2.djtsy,0) as ssr",
        "t1.xmset as xmset",
        "nvl(t3.ssqset,'') as ssqset",
        "nvl(t4.sshset,'') as sshset"
      )
    FACT_TEST_SSQ_DATA.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_SSQ_DATA")
    FACT_TEST_SSH_DATA.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_SSH_DATA")
    FACT_TEST_GBJB_XMTF.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_GBJB_XMTF")
    FACT_TEST_SSR_DATA.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_SSR_DATA")
  }
}
