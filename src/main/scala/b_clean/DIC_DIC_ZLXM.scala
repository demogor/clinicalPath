package b_clean

import org.apache.spark.sql.SparkSession

object DIC_DIC_ZLXM {
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
        |CREATE TABLE IF NOT EXISTS clinical_path.DIC_DIC_ZLXM
        | (bbm varchar(100),
        |zt varchar(100),
        |xxqxrq varchar(100),
        |xxsxrq varchar(100),
        |wjbm varchar(100),
        |zfbf varchar(100),
        |xmmc varchar(100),
        |xmnh varchar(100),
        |cwnr varchar(100),
        |jjdw varchar(100),
        |sfbz varchar(100),
        |bz varchar(100),
        |xdnr varchar(100),
        |fylb varchar(100))
      """.stripMargin)
    spark.sql(
      """
        |truncate table clinical_path.DIC_DIC_ZLXM
      """.stripMargin)
    spark.sql(
      """
        |insert into clinical_path.DIC_DIC_ZLXM
        |  select trim(t1.ybbm) as ybbm,
        |   trim(t1.zt) as zt,
        |   trim(t1.xxqxrq) as xxqxrq,
        |   trim(t1.xxsxrq) as xxsxrq,
        |   trim(t1.wjbm) as wjbm,
        |   trim(t1.zfbf) as zfbf,
        |   trim(t1.xmmc) as xmmc,
        |   trim(t1.xmnh) as xmnh,
        |   trim(t1.cwnr) as cwnr,
        |   trim(t1.jjdw) as jjdw,
        |   trim(t1.sfbz) as sfbz,
        |   trim(t1.bz) as bz,
        |   trim(t1.xdnr) as xdnr,
        |   trim(t1.fylb) as fylb
        | from clinical_path.SUM_TB_DIC_ZLXM t1
        | where trim(t1.wjbm) <>''
      """.stripMargin)
    spark.stop()
  }
}
