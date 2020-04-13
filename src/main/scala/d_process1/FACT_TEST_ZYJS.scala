package d_process1

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, max, sum}
import org.apache.spark.sql.types.TimestampType

object FACT_TEST_ZYJS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //单据数据和明细项目数据
    val fact_dic_zyjs = spark.sql("select t1.* from clinical_path.fact_dic_zyjs t1 where zdbm='N20.100'")
    val fact_dic_mxxm = spark.sql("select lsh,jgdm,mxxmbm,mxxmsysj from clinical_path.fact_dic_mxxm where zdbm='N20.100'")
    val dic_dic_yljg=spark.sql("select * from clinical_path.dic_dic_yljg")

    //单据中住院多天但是只有一天的明细交易
    val fact_test_zyjl_df0 = fact_dic_mxxm.groupBy("lsh").agg(countDistinct("mxxmsysj") as "cnt").
      filter("cnt>1").as("t1").
      join(fact_dic_zyjs.as("t2"), $"t1.lsh" === $"t2.lsh", "inner").selectExpr(
      "t1.lsh as lsh",
      "t2.zyts as zyts").filter("ceil(zyts)=1")

    val fact_test_zyjl_int = fact_dic_zyjs.as("t1").
      join(fact_test_zyjl_df0.as("t2"), $"t1.lsh" === $"t2.lsh", "left").
      join(dic_dic_yljg.as("t3"),$"t1.yydm"===$"t3.Hospital_id","left").
      filter("t2.lsh is null").selectExpr(
      "t1.*",
      "t3.name as yymc",
      "floor((UNIX_TIMESTAMP(current_date) - UNIX_TIMESTAMP(substr(t1.sfzh, 7, 8), 'yyyyMMdd')) /(365 * 24 * 60 * 60)) as age",
      "case when length(t1.sfzh) = 18 then if(substr(t1.sfzh, 17, 1) %2 = 0, '2', '1')  when length(t1.sfzh) = 15 then  if(substr(t1.sfzh, 15) %2 = 0, '2', '1') else '1' end as sex"
    )
    //保留住院天数的单尾95%以内的数据
    val V_outline = fact_test_zyjl_int.selectExpr("avg(zyts)+1.645*stddev_pop(zyts) as V_outline").collect
    val V_outline1 = V_outline.map {case Row(mxxm: Double) => mxxm}
    val fact_test_zyjl_int1 = fact_test_zyjl_int.filter($"zyts" < V_outline1(0)).withColumn("jsqsr", $"jsqsr".cast(TimestampType))

    //单据中住院多天但是只有一天的明细交易
    fact_test_zyjl_int.write.format("orc").mode("Overwrite").saveAsTable("clinical_path.fact_test_zyjl_int")
    // 单据中住院多天但是只有一天的明细交易 和 保留住院天数的单尾95%以内的数据
    fact_test_zyjl_int1.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_zyjl")
  }
}
