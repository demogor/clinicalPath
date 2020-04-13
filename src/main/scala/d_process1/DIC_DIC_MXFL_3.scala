package d_process1

import org.apache.spark.sql.SparkSession

object DIC_DIC_MXFL_3 {
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
        |drop table if exists clinical_path.DIC_DIC_MXFL_3
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.DIC_DIC_MXFL_3 as
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
        |				'诊疗服务1' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join clinical_path.SUM_TB_DIC_JBYLFW_JS t2
        |    on substr(t1.wjbm, 1, 2) = t2.sjbm and substr(t2.xmbm,1,2) not in ('31','33')
        |union all
        |select distinct trim(t1.ybbm) as ybbm,
        |                trim(t2.sjbm) as sjmlbm,
        |                trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.sjmc, '^\\d+\\.', ''),
        |                                    '^\\d+\\．',
        |                                    '')) as sjmlmc,
        |				'诊疗服务1' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join clinical_path.SUM_TB_DIC_JBYLFW_JS t2
        |    on substr(t1.wjbm, 1, 4) = t2.sjbm and substr(t2.xmbm,1,2) not in ('31','33')
        |union all
        |select distinct trim(t1.ybbm) as ybbm,
        |                trim(t2.sjbm) as sjmlbm,
        |                trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.sjmc, '^\\d+\\.', ''),
        |                                    '^\\d+\\．',
        |                                    '')) as sjmlmc,
        |				'诊疗服务1' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join clinical_path.SUM_TB_DIC_JBYLFW_JS t2
        |    on substr(t1.wjbm, 1, 6) = t2.sjbm and substr(t2.xmbm,1,2) not in ('31','33')
        |union all
        |select distinct trim(t1.ybbm) as ybbm,
        |                t2.xmbm as sjmlbm,
        |                trim(REGEXP_REPLACE(REGEXP_REPLACE(t2.xmmc, '^\\d+\\.', ''),
        |                                    '^\\d+\\．',
        |                                    ''))as sjmlmc,
        |				'诊疗服务2' as sdmlbz
        |  from clinical_path.DIC_DIC_ZLXM t1
        | inner join
        | (select substr(trim(xmbm),1,9) as xmbm,max(xmmc) as xmmc from clinical_path.SUM_TB_DIC_JBYLFW_JS group by substr(trim(xmbm),1,9)) t2
        |    on  substr(trim(t1.wjbm),1,9)=t2.xmbm and substr(t2.xmbm,1,2) in ('31','33')
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
