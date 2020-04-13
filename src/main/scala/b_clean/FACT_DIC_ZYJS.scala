package b_clean

import org.apache.spark.sql.SparkSession

object FACT_DIC_ZYJS {
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
        | drop table clinical_path.fact_dic_zyjs
      """.stripMargin)

    spark.sql(
      """
        |create table clinical_path.fact_dic_zyjs
        |  select trim(t1.lsh) as lsh,
        |         trim(t1.zhh) as zhh,
        |         trim(t1.kh) as kh,
        |         trim(t1.zhbz) as zhbz,
        |         trim(t1.yydm) as yydm,
        |         trim(t1.zddm) as zddm,
        |         t1.jsqsr as jsqsr,
        |         nvl(t1.zyts, 0) as zyts,
        |         trim(t1.ksdm) as ksdm,
        |         trim(t1.ksmc) as ksmc,
        |         trim(t1.zyh) as zyh,
        |         trim(t1.zdbm) as zdbm,
        |         trim(t1.zflx) as zflx,
        |         nvl(t1.fyze, 0) as fyze,
        |         nvl(t1.qfdxj_zfs, 0) as qfdxj_zfs,
        |         nvl(t1.qfdzh_zfs, 0) as qfdzh_zfs,
        |         nvl(t1.tcdxj_zfs, 0) as tcdxj_zfs,
        |         nvl(t1.tcdzh_zfs, 0) as tcdzh_zfs,
        |         nvl(t1.tc_zfs, 0) as tc_zfs,
        |         nvl(t1.fjdxj_zfs, 0) as fjdxj_zfs,
        |         nvl(t1.fjdzh_zfs, 0) as fjdzh_zfs,
        |         nvl(t1.fj_zfs, 0) as fj_zfs,
        |         t1.jsqsr as jyrq,
        |         trim(t1.ztbz) as ztbz,
        |         trim(t1.sfzh) as sfzh,
        |         trim(t1.xm) as xm,
        |         trim(t1.xb) as xb,
        |         trim(t1.rylbdm) as rylbdm,
        |         trim(t1.dwh) as dwh,
        |         trim(t1.dwxz) as dwxz,
        |         trim(t1.dwlx) as dwlx,
        |         trim(t1.yy_dj) as yy_dj,
        |         trim(t1.yy_qxdm) as yy_qxdm,
        |         trim(t1.zhbz_1) as zhbz_1,
        |         trim(t1.zhbz_2) as zhbz_2,
        |         trim(t1.zhbz_3) as zhbz_3,
        |         trim(t1.zhbz_4) as zhbz_4,
        |         trim(t1.zhbz_5) as zhbz_5,
        |         trim(t1.zhbz_6) as zhbz_6,
        |         trim(t1.zhbz_7) as zhbz_7,
        |         trim(t1.zhbz_8) as zhbz_8,
        |         trim(t1.zhbz_9) as zhbz_9,
        |         trim(t1.zhbz_10) as zhbz_10,
        |         trim(t1.zhbz_11) as zhbz_11,
        |         trim(t1.zhbz_12) as zhbz_12,
        |         trim(t1.zhbz_13) as zhbz_13,
        |         trim(t1.zhbz_14) as zhbz_14,
        |         trim(t1.zhbz_15) as zhbz_15,
        |         trim(t1.zhbz_16) as zhbz_16,
        |         nvl(t1.klx, 0) as klx,
        |         nvl(t1.ybnd, 0) as ybnd,
        |         nvl(t1.ybjd, 0) as ybjd,
        |         nvl(t1.jd, 0) as jd,
        |         nvl(t1.week, 0) as week,
        |         nvl(t1.jyr, 0) as jyr,
        |         nvl(t1.m_flag, 0) as m_flag,
        |         nvl(t1.nd, 0) as nd,
        |         trim(t1.ny) as ny
        |    from clinical_path.sum_tb_zyzf_jyjl t1 where nvl(t1.zyts, 0)>0
        |    and zdbm='N20.100'
      """.stripMargin)
    spark.stop()
  }
}
