package c_pressess1

import org.apache.spark.sql.SparkSession

object FACT_TEST_LDA2_RES {
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
        |drop table if exists clinical_path.FACT_TEST_LDA2_RES0
      """.stripMargin)
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_TEST_LDA2_RES1
      """.stripMargin)
    spark.sql(
      """
        |drop table if exists clinical_path.FACT_TEST_LDA2_RES2
      """.stripMargin)
    spark.sql(
      """
        |drop table if exists clinical_path.fact_test_lda2_res2_0
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA2_RES0 as
        | select t.lsh,concat_ws('-',collect_list(t.djtsy)) as djtsy
        | ,concat_ws('-',collect_list(cast(t.topic as string))) as topic from
        | (select * from clinical_path.fact_test_lda_ladmodel_2_1 order by cast(djtsy as int) ) t group by t.lsh
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA2_RES1 as
        | select t.djtsy,t.topic,t.zyts,count(1) as sl from (
        | select t1.lsh,t1.djtsy,t1.topic,t3.zyts from clinical_path.FACT_TEST_LDA2_RES0 t1
        | inner join  (select distinct lsh,zyts from clinical_path.FACT_TEST_LDA_DATA_2 )t3
        |on t3.lsh=t1.lsh) t group by t.djtsy,t.topic,t.zyts
      """.stripMargin)
    //**********为了对后后续的卡方检验去除小概率的临床路径，即减去概率由小到大前20%且要满足均大于1次的临床路径
    //**********之后人次小的前10%的医院且要满足均大于1人次的医院
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA2_RES1_1 as
        |select t.topic,t.sl,row_number() over (order by sl) rank,row_number() over (order by t.sl)/count(1) over() as ratio from
        | (select t1.topic as topic,count(1) as sl from  clinical_path.FACT_TEST_LDA2_RES0 t1
        |where exists(select 1 from  (select distinct lsh as lsh from clinical_path.FACT_TEST_LDA_DATA_2 where zyts<2) t3
        |where  t3.lsh=t1.lsh ) group by t1.topic) t
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA2_RES2_1 as
        |select t2.lsh as lsh,
        |t2.topic as clinical_path,
        |t3.yydm as yydm,
        |t4.name as yymc,
        |t4.hos_class as yydj
        |from
        |(select * from clinical_path.FACT_TEST_LDA2_RES1_1 where ratio>0.2 and sl<>1)  t1
        |inner join clinical_path.FACT_TEST_LDA2_RES0 t2 on t1.topic=t2.topic
        |inner join clinical_path.FACT_DIC_ZYJS t3 on
        |t2.lsh=t3.lsh
        |inner join clinical_path.DIC_DIC_YLJG t4
        |on t4.hospital_id=t3.yydm
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA2_RES2_2 as
        |select t1.* from  clinical_path.FACT_TEST_LDA2_RES2_1 t1
        |where not exists (
        |select 1 from
        |(select yydm from (
        |select yydm ,cnt,
        |row_number() over(order by cnt) as rank ,
        |row_number() over(order by cnt)/count(1) over() as ratio
        |from (select yydm,count(1) as cnt from clinical_path.FACT_TEST_LDA2_RES2_1 group by yydm) t )t where t.ratio<0.2 or t.cnt=1) t2
        |where t1.yydm=t2.yydm)
      """.stripMargin)
    //**********


    spark.sql(
      """
        |create table clinical_path.FACT_TEST_LDA2_RES2 as
        |select t1.*,t3.zyts,t2.sl,t2.sl/count(1) over() percent from clinical_path.FACT_TEST_LDA2_RES0 t1
        |left join (select distinct lsh,zyts from clinical_path.FACT_TEST_LDA_DATA_2 )t3
        |on t3.lsh=t1.lsh
        |inner join clinical_path.FACT_TEST_LDA2_RES1 t2
        |on t1.djtsy=t2.djtsy and t1.topic=t2.topic and t2.zyts=t3.zyts
      """.stripMargin)
    spark.sql(
      """
        |create table clinical_path.fact_test_lda2_res2_0 as
        |select t1.clinicalPath,
        |t1.cnt,
        |row_number() over(order by t1.cnt) as rank,
        |round(row_number() over(order by t1.cnt)/count(*) over() ,4) as ratio
        |from  (select concat(topic,'*',zyts) as clinicalPath,count(1) as cnt from clinical_path.fact_test_lda2_res2 group by concat(topic,'*',zyts)) t1
      """.stripMargin)
  }
}
