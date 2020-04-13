package d_process1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FACT_TEST_YYSUM {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_dic_zyjs = spark.sql("select t1.* from clinical_path.fact_test_zyjl t1 ")
    val fact_dic_mxxm = spark.sql("select lsh,jgdm,mxxmbm,mxxmybjsfy,ny,zdbm from clinical_path.fact_dic_mxxm")
    val dic_dic_yljg = spark.sql("select * from clinical_path.dic_dic_yljg")

    //药品费

    val FACT_TEST_YYSUM1 =fact_dic_mxxm.
      filter(substring($"mxxmbm",1,1).isin("X","Y","Z")).
      groupBy("jgdm", "ny","zdbm").
      agg(sum("mxxmybjsfy") as "fy")

    //诊疗服务费用
    val FACT_TEST_YYSUM2 =fact_dic_mxxm.
      filter(substring($"mxxmbm",1,1).isin("S","F")).
      groupBy("jgdm", "ny","zdbm").
      agg(sum("mxxmybjsfy") as "fy")

    //材料费
    val FACT_TEST_YYSUM3 =fact_dic_mxxm.
      filter(substring($"mxxmbm",1,1)==="C").
      groupBy("jgdm", "ny","zdbm").
      agg(sum("mxxmybjsfy") as "fy")

    val FACT_TEST_YYSUM4 = fact_dic_zyjs.groupBy("yydm","yymc", "ny","zdbm").
      agg(count("lsh") as "zrc",
        sum("fyze") as "zfy",
        avg("zyts") as "cjzyts",
        avg($"fyze"/$"zyts") as "rjfy"
      ).as("t1").
      join(FACT_TEST_YYSUM1.as("t2"),$"t1.yydm"===$"t2.jgdm" && $"t1.zdbm"===$"t2.zdbm" && $"t1.ny"===$"t2.ny", "left").
      join(FACT_TEST_YYSUM2.as("t3"),$"t1.yydm"===$"t3.jgdm" && $"t1.zdbm"===$"t3.zdbm" && $"t1.ny"===$"t3.ny", "left").
      join(FACT_TEST_YYSUM3.as("t4"),$"t1.yydm"===$"t4.jgdm" && $"t1.zdbm"===$"t4.zdbm" && $"t1.ny"===$"t4.ny", "left").selectExpr(
      "t1.yydm as yydm",
      "t1.yymc as yymc",
      "t1.zfy as zfy",
      "t1.zrc as zrc",
      "t1.cjzyts as cjzyts",
      "t1.rjfy as rjfy",
      "t2.fy as ypfy",
      "t3.fy as zlfy",
      "t3.fy as hcfy",
      "t1.zdbm as zdbm",
      "'' as zdmc",
      "t1.ny as tjny"
    )
    FACT_TEST_YYSUM4.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_YYSUM")
  }
}
