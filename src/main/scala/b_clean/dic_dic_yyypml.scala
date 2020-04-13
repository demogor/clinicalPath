package b_clean

import org.apache.spark.sql.SparkSession

object dic_dic_yyypml {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql(
      """
        |drop table if exits clinical_path.dic_dic_yyypml
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.dic_dic_yyypml as
        | select t1.*,t2.flmc,t2.flbm from clinical_path.sum_tb_dic_yyypml t1
        | inner join clinical_path.SUM_TB_DIC_YPML t2
        | on t1.mlsxbm=t2.mlsxh and (case when t1.zxybz='中成药' then "Z" else "X" end)=substr(t2.flbm,1,1)
      """.stripMargin)
  }
}
