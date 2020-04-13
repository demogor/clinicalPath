package b_clean

import org.apache.spark.sql.SparkSession

object DIC_DIC_MXFL {
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
        |drop table if exists clinical_path.DIC_DIC_MXFL
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.DIC_DIC_MXFL as
        |--药品
        |  select t1.ypbm as ybbm , t2.flbm as sjmlbm,
        |        trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.flmc,'^\\d+\\.',''),'^\\d+\\．','')) as sjmlmc,
        |       '药品' as sdmlbz
        |          from clinical_path.sum_tb_dic_yyypml t1
        |         inner join clinical_path.SUM_TB_DIC_YPML t2
        |            on t1.mlsxbm = t2.mlsxh
        |           and (case
        |                 when t1.zxybz = '中成药' then
        |                  "Z"
        |                 else
        |                  "X"
        |               end) = substr(t2.flbm, 1, 1)
        |--诊疗服务
        |union all
        |select distinct trim(t1.ybbm) as ybbm,
        |                trim(t2.sjbm) as sjmlbm,
        |                trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.sjmc, '^\\d+\\.', ''),
        |                                    '^\\d+\\．',
        |                                    '')) as sjmlmc,
        |				'诊疗服务' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join clinical_path.SUM_TB_DIC_JBYLFW t2
        |    on substr(t1.wjbm, 1, 2) = t2.sjbm
        |union all
        |select distinct trim(t1.ybbm) as ybbm,
        |                trim(t2.sjbm) as sjmlbm,
        |                trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.sjmc, '^\\d+\\.', ''),
        |                                    '^\\d+\\．',
        |                                    '')) as sjmlmc,
        |				'诊疗服务' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join clinical_path.SUM_TB_DIC_JBYLFW t2
        |    on substr(t1.wjbm, 1, 4) = t2.sjbm
        |union all
        |select distinct trim(t1.ybbm) as ybbm,
        |                trim(t2.sjbm) as sjmlbm,
        |                trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.sjmc, '^\\d+\\.', ''),
        |                                    '^\\d+\\．',
        |                                    '')) as sjmlmc,
        |				'诊疗服务' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join clinical_path.SUM_TB_DIC_JBYLFW t2
        |    on substr(t1.wjbm, 1, 6) = t2.sjbm
        |--器材
        |union all
        |select  distinct trim(ybbm) as ybbm,
        |				trim(sanjml) as sjmlbm,
        |			trim(REGEXP_REPLACE(REGEXP_REPLACE(sanjml,'^\\d+\\.',''),'^\\d+\\．','')) as sjmlmc,
        |         '器材' as sdmlbz
        |  from clinical_path.sum_tb_dic_ylqc
        | where trim(sanjml)<>''
      """.stripMargin)
  }
}
