package b_clean

import org.apache.spark.sql.SparkSession

object FACT_DIC_RYZD {
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
        |CREATE TABLE IF NOT EXISTS clinical_path.FACT_DIC_RYZD
        | (lsh varchar(16),
        | jgdm varchar(16),
        | kh varchar(10),
        | zyh varchar(16),
        | bcjszyts decimal(5,1),
        | zyjsksrq varchar(8),
        | ryzdbm varchar(7),
        | ryzdsm varchar(30),
        | zyjsjsrq varchar(8),
        | jgzybz varchar(1),
        | cybz varchar(1),
        | jsrqsj varchar(16),
        | drsj date,
        | jlhh decimal(6,0),
        | wjmc varchar(23),
        | jyrq decimal(8,0),
        | m_flag decimal(2,0),
        | ny varchar(6),
        | cwbz varchar(1))
      """.stripMargin)
    spark.sql(
      """
        |truncate table clinical_path.FACT_DIC_RYZD
      """.stripMargin)
    spark.sql(
      """
        |insert into clinical_path.FACT_DIC_RYZD
        |  select trim(t1.lsh) as lsh,
        |         trim(t1.jgdm) as jgdm,
        |         trim(t1.kh) as kh,
        |         trim(t1.zyh) as zyh,
        |         nvl(t1.bcjszyts, 0) as bcjszyts,
        |         trim(t1.zyjsksrq) as zyjsksrq,
        |         trim(t1.ryzdbm) as ryzdbm,
        |         trim(t1.ryzdsm) as ryzdsm,
        |         trim(t1.zyjsjsrq) as zyjsjsrq,
        |         trim(t1.jgzybz) as jgzybz,
        |         trim(t1.cybz) as cybz,
        |         trim(t1.jsrqsj) as jsrqsj,
        |         t1.drsj as drsj,
        |         nvl(t1.jlhh, 0) as jlhh,
        |         trim(t1.wjmc) as wjmc,
        |         nvl(t1.jyrq, 0) as jyrq,
        |         nvl(t1.m_flag, 0) as m_flag,
        |         trim(t1.ny) as ny,
        |         trim(t1.cwbz) as cwbz
        |    from clinical_path.TB_HOS_JGZY_CRYK_ALL t1
      """.stripMargin)
  }
}
