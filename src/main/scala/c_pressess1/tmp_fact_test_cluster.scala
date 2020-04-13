package c_pressess1

import org.apache.spark.sql.SparkSession

object tmp_fact_test_cluster {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    try {
      spark.sql(
        """
          |drop table if exists clinical_path.tmp_fact_test_cluster
        """.stripMargin)

    spark.sql(
      """
        |create table clinical_path.tmp_fact_test_cluster as
        |select t.zyts, t.fylb, round(avg(fy), 3) as fyze
        |  from (select t1.lsh, t1.zyts, t2.fylb, t2.fy
        |          from clinical_path.fact_test_zyjl t1
        |          left join (select lsh,
        |                           substr(mxxmbm, 1, 1) as fylb,
        |                           sum(mxxmybjsfy) as fy
        |                      from clinical_path.fact_dic_mxxm
        |                     group by lsh, substr(mxxmbm, 1, 1)) t2
        |            on t1.lsh = t2.lsh
        |         WHERE t1.zyts > 0) t
        | group by t.zyts, t.fylb
      """.stripMargin)}
      finally {
        spark.stop()
      }
  }
}
