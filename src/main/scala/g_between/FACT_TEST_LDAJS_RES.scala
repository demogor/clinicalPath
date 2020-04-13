package g_between

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FACT_TEST_LDAJS_RES {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //获取接口数据的 记录数据(获取每一个lsh的医院代码)
    val FACT_TEST_LDAJS_DATA = spark.sql(
      """
        |select distinct yydm,yymc,yydj,lsh,zyts from clinical_path.FACT_TEST_LDAJS_DATA
      """.stripMargin)
    //医院字典表（获取每一个lsh的医院名称）

    //每一个病例形成路径 1-3-4，1-2-4（不带有1-2-2-4）
    val FACT_TEST_LDA2_RES0 = spark.sql(
      """
        |select t.lsh as lsh, --流水号
        |concat_ws('-',collect_set(t.djtsy)) as djtsy, --第几天使用
        |concat_ws('-',collect_set(cast(t.topic as string))) as topic  --临床路径
        |from(select * from clinical_path.fact_test_ldajs_ladmodel order by cast(djtsy as int) ) t
        |group by t.lsh
      """.stripMargin)



//    得到每个路径（topic）的个数，并对其排序和计算发生的概率
    val FACT_TEST_LDA2_RES1 = FACT_TEST_LDA2_RES0.
      groupBy("topic").agg(count("lsh") as "sl").selectExpr(
      "topic as topic",
      "sl as sl", //出现次数
      "row_number() over (order by sl) as rank", //排名
      "row_number() over (order by sl)/count(1) over() as ratio" //排名的占位
      //去除某些出现较少的路径
    ).filter("ratio>0.2 and sl<>1").as("t1").
      join(FACT_TEST_LDA2_RES0.as("t2"), $"t1.topic" === $"t2.topic", "inner").
      join(FACT_TEST_LDAJS_DATA.as("t3"), $"t2.lsh" === $"t3.lsh", "inner").selectExpr(
      "t2.lsh as lsh",
      "t2.topic as clinical_path",
      "t3.yydm as yydm", //医院代码
      "t3.yymc as yymc", //医院名称
      "t3.yydj as yydj" //医院等级
    )
    //去除不太好的医院
    val FACT_TEST_LDA2_RES2 = FACT_TEST_LDA2_RES1.groupBy("yydm").agg(count("lsh") as "cnt").selectExpr(
      "yydm as yydm",
      "cnt as cnt",
      "row_number() over(order by cnt) as rank",
      "row_number() over(order by cnt)/count(1) over() as ratio"
    ).filter("ratio>0.2 and cnt<>1")

    val FACT_TEST_LDA2_RES3= FACT_TEST_LDA2_RES1.as("t1").
      join(FACT_TEST_LDA2_RES2.as("t2"), $"t1.yydm" === $"t2.yydm", "inner").selectExpr(
      "t1.lsh as lsh",
      "t1.clinical_path as clinical_path",
      "t1.yydm as yydm", //医院代码
      "t1.yymc as yymc", //医院名称
      "t1.yydj as yydj" //医院等级
    )
    FACT_TEST_LDA2_RES3.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_ldajs_res")
  }
}
