package c_pressess1

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types._



object FACT_TEST_DIFF_TEST {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_diff_data_df = spark.sql(
      """
        |select yydj, yydm,t.clinical_path as clinicalPath
        |from clinical_path.FACT_TEST_LDA2_RES2_2 t
      """.stripMargin).cache()

    //建立空表
    val myschema = StructType(List(
      StructField("yydm", StringType),
      StructField("pValue", DoubleType)))
    var fact_test_diff_test_res0 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], myschema)

    //val A = "42502657200"
    val yydm = fact_test_diff_data_df.select($"yydm").distinct.collect
    val YYDM1 = yydm.map { case Row(mxxm: String) => mxxm }
    for (i <- 0 to YYDM1.length - 1) {
      val A = YYDM1(i)
      //取该医院所属等级
      val yydj = fact_test_diff_data_df.filter($"yydm" === A).select($"yydj").distinct.collect
      val yydj1 = yydj.map { case Row(mxxm: String) => mxxm }
      if (yydj1.length == 1) {
        //把该医院转换为1，同等级其他医院的转化为2
        val fact_test_diff_data_df_1 = fact_test_diff_data_df.filter($"yydj" === yydj1(0)).map(x => {
          val yydm = x.getString(1) match {
            case A => 1
            case _ => 2
          }
          (yydm, x.getString(2))
        }).toDF("id", "category")
        //把字符串转换为数字
        val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
        val indexed = indexer.fit(fact_test_diff_data_df_1).transform(fact_test_diff_data_df_1)
        val model_data = indexed.map(x => (x.getInt(0), Vectors.dense(x.getDouble(2)))).toDF("id", "categoryIndex")
        //卡方检验
        val chi = ChiSquareTest.test(model_data, "categoryIndex", "id").head
        val pValue = chi.getAs[DenseVector](0).toArray(0)
        //保存结果
        val data_result = spark.sparkContext.parallelize(Seq((A, pValue))).toDF("id", "pValue")
        fact_test_diff_test_res0 = fact_test_diff_test_res0.union(data_result)
      }
    }
    fact_test_diff_test_res0.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_diff_test_res0")
  }
}
