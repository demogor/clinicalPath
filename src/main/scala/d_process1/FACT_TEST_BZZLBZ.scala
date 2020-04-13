package d_process1

import org.apache.spark.sql.SparkSession

object FACT_TEST_BZZLBZ {
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
        |create table if not exists clinical_path.FACT_TEST_BZZLBZ(
        |zdbm varchar(16),
        |zdmc varchar(50),
        |ssmc varchar(50),
        |mzfs varchar(50),
        |bzzyr int,
        |sqzb int,
        |ssr int,
        |shhf int,
        |yxbz int)
      """.stripMargin)
    spark.sql(
      """
        |INSERT INTO clinical_path.fact_test_bzzlbz VALUES (
        |'N20.100'	,
        |'输尿管结石'	,
        |'经输尿管镜碎石取石术'	,
        |'硬膜外麻醉或全麻'	,
        |5	,
        |2	,
        |3	,
        |2	,
        |1
        |)
      """.stripMargin)
    spark.sql(
      """
        |INSERT INTO clinical_path.fact_test_bzzlbz VALUES (
        |'H11.001'	,
        |'翼状胬肉手术'	,
        |'翼状胬肉切除术'	,
        |'表面麻醉或联合局部注射浸润麻醉'	,
        |3	,
        |2	,
        |2	,
        |2	,
        |1
        |)
      """.stripMargin)
    spark.sql(
      """
        |INSERT INTO clinical_path.fact_test_bzzlbz VALUES (
        |'H25.901'	,
        |'老年性白内障'	,
        |'超声乳化白内障摘除术+人工晶体植入术（IOL）'	,
        |'表面麻醉或球后/球周阻滞麻醉'	,
        |6	,
        |2	,
        |6	,
        |3	,
        |1
        |)
      """.stripMargin)
  }
}
