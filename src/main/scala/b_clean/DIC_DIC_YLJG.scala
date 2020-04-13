package b_clean

import org.apache.spark.sql.SparkSession

object DIC_DIC_YLJG {
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
        |CREATE TABLE IF NOT EXISTS clinical_path.DIC_DIC_YLJG
        | (hospital_id varchar(16),
        |  name varchar(60),
        |  short_name varchar(60),
        |  hos_class varchar(1),
        |  area_id varchar(2),
        |  area_hos_no varchar(2),
        |  property varchar(2),
        |  address varchar(100),
        |  postcode varchar(6),
        |  telephone varchar(20),
        |  contact_person varchar(32),
        |  valid_flag varchar(1),
        |  qualification varchar(32),
        |  yxjsksrq decimal(8,0),
        |  yxjsjsrq decimal(8,0),
        |  num_patients decimal(8,0))
      """.stripMargin)
    spark.sql(
      """
        |truncate table clinical_path.DIC_DIC_YLJG
      """.stripMargin)
    spark.sql(
      """
        |insert into clinical_path.DIC_DIC_YLJG
        |  select trim(t1.hospital_id) as hospital_id,
        |         trim(t1.name) as name,
        |         trim(t1.short_name) as short_name,
        |         trim(t1.hos_class) as hos_class,
        |         trim(t1.area_id) as area_id,
        |         trim(t1.area_hos_no) as area_hos_no,
        |         trim(t1.property) as property,
        |         trim(t1.address) as address,
        |         trim(t1.postcode) as postcode,
        |         trim(t1.telephone) as telephone,
        |         trim(t1.contact_person) as contact_person,
        |         trim(t1.valid_flag) as valid_flag,
        |         trim(t1.qualification) as qualification,
        |         nvl(t1.yxjsksrq, 0) as yxjsksrq,
        |         nvl(t1.yxjsjsrq, 0) as yxjsjsrq
        |         t2.num_patients as num_patients
        |    from clinical_path.SUM_TB_DIC_YLJG_YY t1
        |    left join (select yydm,count(distinct lsh) as num_patients from
        |    clinical_path.fact_test_zyjl_int group by  yydm) t2
        |    on trim(t1.hospital_id)=t2.yydm
      """.stripMargin)
    spark.stop()
  }
}
