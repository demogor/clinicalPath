package e_cluster

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object FACT_TEST_JGCV {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_zyjl=spark.sql(
      """
        |select yydm, lsh, zyts from clinical_path.fact_test_zyjl where zdbm='N20.100'
      """.stripMargin)
    val dic_dic_yljg=spark.sql(
      """
        |select Hospital_id as yydm,name as yymc, Hos_class as yydj
        |    from clinical_path.dic_dic_yljg
      """.stripMargin)
    val windowExpr = Window.partitionBy().orderBy(("cnt"))

    val FACT_TEST_JGCV=fact_test_zyjl.groupBy("yydm").agg(
      countDistinct("lsh") as "cnt",
      round(stddev_pop("zyts") / avg("zyts") ,4) as "cv"
    ).as("t1").join(dic_dic_yljg.as("t2"),$"t1.yydm"===$"t2.yydm","left").selectExpr(
      "t1.yydm as yydm",
      "t2.yymc as yymc",
      "t2.yydj as yydj",
      "t1.cnt as zrc",
      "t1.cv as cv",
      "ROW_NUMBER() over(partition by t2.yydj order by t1.cnt asc) as rank_cnt",
      "ROW_NUMBER() over(partition by t2.yydj order by t1.cv desc) as rank_cv"
    )
    FACT_TEST_JGCV.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_jgcv")
  }
}
