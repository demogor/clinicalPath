package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_GBJD_YS_RES {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_TEST_GBJD_YS_RES0
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_GBJD_YS_RES0 as
        |select t1.lsh,t1.zyts,t1.ssr,
        |round(t1.ssqsimi,3) as ssqsimi,
        |round(t1.sshsimi,3) as sshsimi,
        |t2.yydm,nvl(t3.total,0) as Total
        |from clinical_path.FACT_TEST_SS_RES t1
        |left join
        |(select * from clinical_path.FACT_DIC_ZYJS where zdbm='N20.100') t2
        |on t1.lsh=t2.lsh
        |left join clinical_path.FACT_TEST_GBJD_RES t3
        |on t1.lsh=t3.lsh
      """.stripMargin)
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_TEST_GBJD_YS_RES
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_GBJD_YS_RES as
        |select t1.yydm,t3.name,t1.count1 as zrc,
        |t1.count1-nvl(count0,0) as nssrc ,
        |round((nvl(count0,0))/t1.count1,3) as ssrczb,
        |round(nvl(t2.avgTotal,0),3) as avgMatch
        |from (select yydm,count(1) as count1 from clinical_path.FACT_TEST_GBJD_YS_RES0 group by yydm) t1
        |left join (
        |select yydm,count(1) as count0, avg(total) as avgTotal from clinical_path.FACT_TEST_GBJD_YS_RES0 t1 where ssr>0 group by yydm) t2
        |on t1.yydm=t2.yydm
        |left join  clinical_path.DIC_DIC_YLJG t3
        |on t1.yydm=t3.hospital_id
      """.stripMargin)
  }
}
