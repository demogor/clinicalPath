package b_clean

import org.apache.spark.sql.SparkSession

object FACT_DIC_MXXM {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    //支撑重跑
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_DIC_MXXM
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_DIC_MXXM as
        |  select trim(t1.lsh) as lsh,
        |         trim(t1.jgdm) as jgdm,
        |         trim(t1.kh) as kh,
        |         trim(t1.ksbm) as ksbm,
        |         trim(t1.ksmc) as ksmc,
        |         trim(t1.ysgh) as ysgh,
        |         trim(t1.ysxm) as ysxm,
        |         trim(t1.fylb) as fylb,
        |         trim(t1.mxxmbm) as mxxmbm,
        |         trim(t1.mxxmmc) as mxxmmc,
        |         trim(t1.mxxmdw) as mxxmdw,
        |         nvl(t1.mxxmdj,0) as mxxmdj,
        |         nvl(t1.mxxmsl, 0) as mxxmsl,
        |         nvl(t1.mxxmje, 0) as mxxmje,
        |         nvl(t1.mxxmjyfy, 0) as mxxmjyfy,
        |         nvl(t1.mxxmybjsfy, 0) as mxxmybjsfy,
        |         trim(t1.bxbz) as bxbz,
        |         trim(t1.jsrqsj) as jsrqsj,
        |         trim(t1.ybbf) as ybbf,
        |         trim(t1.jslxbz) as jslxbz,
        |         trim(t1.sftfbz) as sftfbz,
        |         t1.drsj as drsj,
        |         nvl(t1.jlhh, 0) as jlhh,
        |         trim(t1.wjmc) as wjmc,
        |         nvl(t1.jyrq, 0) as jyrq,
        |         nvl(t1.m_flag, 0) as m_flag,
        |         trim(t1.ny) as ny,
        |         trim(t1.jfbz) as jfbz,
        |         trim(t1.mxxmxflsh) as mxxmxflsh,
        |         trim(t1.yyclpp) as yyclpp,
        |         trim(t1.zczh) as zczh,
        |         trim(t1.mxxmgg) as mxxmgg,
        |         trim(t1.mxxmsysj) as mxxmsysj,
        |         trim(t1.cwbz) as cwbz,
        |         t2.zdbm as zdbm
        |    from clinical_path.TB_HOS_JGZY_MXK_ALL t1
        |    inner join clinical_path.FACT_DIC_ZYJS t2
        |    on trim(t1.lsh)=t2.lsh
        |    where trim(t1.sftfbz)='1' and mxxmbm <> ''
      """.stripMargin
    )
  }
}
