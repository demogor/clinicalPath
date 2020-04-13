package c_pressess1


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg. Vectors
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object FACT_TEST_DIFF_LM {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //建立空表
    val myschema = StructType(List(
      StructField("yydm", StringType),
      StructField("coef", DoubleType),
      StructField("rs", DoubleType)))
    var fact_test_diff_lm_res0 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], myschema)

    val fact_test_diff_data_df = spark.sql(
      """
        |select yydj, yydm,t.clinical_path as clinicalPath
        |from clinical_path.FACT_TEST_LDA2_RES2_2 t
      """.stripMargin).cache()
    val yydm = fact_test_diff_data_df.select($"yydm").distinct.collect
    val YYDM1 = yydm.map { case Row(mxxm: String) => mxxm }
    for (i <- 0 to YYDM1.length - 1) {
      val A = YYDM1(i)
      //取该医院所属等级
      val yydj = fact_test_diff_data_df.filter($"yydm" === A).select($"yydj").distinct.collect
      val yydj1 = yydj.map { case Row(mxxm: String) => mxxm }
      if (yydj1.length == 1) {
        //把该医院转换为1，同等级其他医院的转化为2
        val fact_test_diff_data_df_1 = fact_test_diff_data_df.filter($"yydm" === A).groupBy($"clinicalPath" as "clinicalPath1").
          agg(count($"yydm") as "cnt1")
        val fact_test_diff_data_df_2 = fact_test_diff_data_df.filter($"yydm" =!= A and $"yydj" === yydj1(0)).groupBy($"clinicalPath" as "clinicalPath2").
          agg(count($"yydm") as "cnt2")
        val fact_test_diff_data_df_3 = fact_test_diff_data_df_1.join(fact_test_diff_data_df_2, $"clinicalPath1" === $"clinicalPath2", "full").selectExpr(
          "nvl(clinicalPath1,clinicalPath2) as clinicalPath", "nvL(cnt1,0) as t1cnt", "nvL(cnt2,0) as t2cnt").
          selectExpr("row_number() over (order by t2cnt desc) as rank", "clinicalPath", "t1cnt as t1cnt", "t2cnt")
        val lr = new LinearRegression()
          .setMaxIter(100)
          .setRegParam(0.001)
          .setElasticNetParam(0.8)
        val modelData = fact_test_diff_data_df_3.select($"rank", $"t1cnt").map { line =>
          (line.getInt(0), Vectors.dense(line.getLong(1)))
        }.toDF("label", "features")
        val lrModel = lr.fit(modelData)
        val trainingSummary = lrModel.summary
        val RS=trainingSummary.r2
        val pValue = lrModel.coefficients(0)
        val data_result = spark.sparkContext.parallelize(Seq((A, pValue,RS))).toDF("yydm", "coef","rs")
        fact_test_diff_lm_res0 = fact_test_diff_lm_res0.union(data_result)
      }
    }
    fact_test_diff_lm_res0.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_diff_lm_res0")
  }
}
