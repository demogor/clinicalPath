package d_process2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object FACT_TEST_factor_data {
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
    val fact_test_zyjl_int = spark.sql("select t1.* from clinical_path.fact_test_zyjl_int t1")
    val fact_dic_mxxm = spark.sql("select lsh,jgdm,mxxmbm,mxxmsysj from clinical_path.fact_dic_mxxm where zdbm='N20.100'")
    val dic_dic_yljg=spark.sql("select * from clinical_path.dic_dic_yljg")

    //因素接口表
    val FACT_TEST_factor_data=fact_dic_mxxm.filter("substr(mxxmbm, 1, 1) in ('C', 'F', 'S', 'X', 'Y', 'Z')").as("t1").
      join(dic_dic_yljg.as("t2"),$"t1.jgdm"===$"t2.hospital_id","inner").selectExpr(
      "substr(t1.mxxmbm, 1, 1) as fylb",
      "t1.lsh as lsh",
      "t2.Hos_class as yydj",
      "t2.hospital_id as yydm",
      "t2.name as yymc",
      "t2.num_patients as zrc",
      "t1.mxxmsysj as fyze"
    ).groupBy("lsh","yydj","yydm","yymc","fylb").agg(
      max("zrc") as "zrc",
      sum("fyze") as "fyze").as("t1").join(
      fact_test_zyjl_int.as("t2"),$"t1.lsh"===$"t2.lsh","inner").selectExpr(
      "t1.yydm as yydm",
      "t1.yymc as yymc",
      "t1.yydj as yydj",
      "t1.zrc as zrc",
      "t2.lsh as lsh",
      "t2.zyts as zyts",
      "t2.age as age",
      "t2.sex as sex",
      "t1.fylb as fylb",
      "t1.fyze as fyze"
    )
    //单据中住院多天但是只有一天的明细交易   的各项目的数据
    FACT_TEST_factor_data.write.mode("Overwrite").saveAsTable("clinical_path.FACT_TEST_factor_data")
  }
}