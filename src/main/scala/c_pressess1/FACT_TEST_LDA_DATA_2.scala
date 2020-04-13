package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_LDA_DATA_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    val zyts = args(0)
    spark.sql(
      """
        |drop table if exists clinical_path.fact_test_lda_data_2
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.fact_test_lda_data_2 as
        |  SELECT t1.lsh as lsh,
        |       t1.mxxmbm as ybbm,
        |       t3.sjmlmc as sjmlmc,
        |       t2.jsqsr as ryrq,
        |       t2.jyr as cyrq,
        |       floor(t2.zyts)+1 as zyts,
        |       t1.mxxmsysj as xmsysj,
        |       datediff(to_date(from_unixtime(unix_timestamp(t1.mxxmsysj,
        |                                                     'yyyymmdd'),
        |                                      'yyyy-mm-dd')),
        |                to_date(t2.jsqsr)) + 1 as djtsy,
        |       t1.mxxmsl as xmsl
        |  FROM clinical_path.fact_dic_mxxm t1
        |inner join (select * from clinical_path.fact_test_zyjl where floor(zyts)+1 <= 13) t2
        |on t1.lsh = t2.lsh
        | --取住院天数为[2,7]
        | --inner join (select * from clinical_path.fact_test_zyjl where floor(zyts)+1 <= 7 and floor(zyts)+1>=2) t2
        | -- on t1.lsh = t2.lsh
        | inner join (select * from clinical_path.dic_dic_mxfl
        | where sjmlmc not in ('诊查费','护理费','诊察费和护理费','静脉注射液','床位费','输液器材','电解质溶液','输液器','注射')) t3
        |    on t1.mxxmbm = t3.ybbm
        | --去除不合格的医院
        | --inner join (select * from clinical_path.FACT_TEST_DIFF_YY where ratio>0.2 and rank>1) t4
        | --on t2.yydm=t4.yydm
      """.stripMargin)
  }
}
