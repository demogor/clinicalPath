package b_clean

import org.apache.spark.sql.SparkSession

object FACT_DIC_CYZD {
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
        |CREATE TABLE IF NOT EXISTS clinical_path.FACT_DIC_CYZD
        | (lsh varchar(16),
        | jgdm varchar(16),
        | kh varchar(10),
        | zyh varchar(16),
        | zyjsjsrq varchar(8),
        | cyzdbm varchar(9),
        | cyzdsm varchar(30),
        | jgzybz varchar(1),
        | zljgdm varchar(1),
        | bz_1 varchar(15),
        | bz_2 varchar(17),
        | jsrqsj varchar(16),
        | drsj date,
        | jlhh decimal(6,0),
        | wjmc varchar(23),
        | jyrq decimal(8,0),
        | m_flag decimal(2,0),
        | ny varchar(6),
        | cwbz varchar(1),
        | zyssczbm varchar(32))
      """.stripMargin)
    //支持重跑
    spark.sql(
      """
        |truncate table clinical_path.FACT_DIC_CYZD
      """.stripMargin)

    spark.sql(
      """
        |insert into clinical_path.FACT_DIC_CYZD
        |  select trim(t1.lsh) as lsh,
        |         trim(t1.jgdm) as jgdm,
        |         trim(t1.kh) as kh,
        |         trim(t1.zyh) as zyh,
        |         trim(t1.zyjsjsrq) as zyjsjsrq,
        |         trim(t1.cyzdbm) as cyzdbm,
        |         trim(t1.cyzdsm) as cyzdsm,
        |         trim(t1.jgzybz) as jgzybz,
        |         trim(t1.zljgdm) as zljgdm,
        |         trim(t1.bz_1) as bz_1,
        |         trim(t1.bz_2) as bz_2,
        |         trim(t1.jsrqsj) as jsrqsj,
        |         t1.drsj as drsj,
        |         nvl(t1.jlhh, 0) as jlhh,
        |         trim(t1.wjmc) as wjmc,
        |         nvl(t1.jyrq, 0) as jyrq,
        |         nvl(t1.m_flag, 0) as m_flag,
        |         trim(t1.ny) as ny,
        |         trim(t1.cwbz) as cwbz,
        |         trim(t1.zyssczbm) as zyssczbm
        |    from clinical_path.TB_HOS_JGZY_CYZDK_ALL t1
      """.stripMargin)
  }
}
