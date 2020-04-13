package f_factor

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{Bucketizer, VectorAssembler}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Random

object FACT_TEST_FACTOR_RES_TR {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_sflb_zyts_df = spark.sql("select * from clinical_path.FACT_TEST_factor_data").
      withColumn("yydj", $"yydj".cast(DataTypes.DoubleType)).
      withColumn("sex", $"sex".cast(DataTypes.DoubleType)).cache()
    //列转行
    val fact_test_sflb_zyts_df1 = fact_test_sflb_zyts_df.
      groupBy($"lsh",$"age",$"sex", $"zyts", $"yydj", $"zrc").
      pivot("fylb", Seq("C", "S", "X", "Y", "Z", "F")).sum("fyze").na.fill(0)
    //因变量处理
    val V_outline = fact_test_sflb_zyts_df1.
      selectExpr("avg(zyts)+1.645*stddev_pop(zyts) as V_outline").collect
    val V_outline1: Array[Double] = V_outline.map { case Row(mxxm: Double) => mxxm }
    val splits = Array(0, V_outline1(0), Double.PositiveInfinity)

    val bucketizer = new Bucketizer().
      setSplits(splits).
      setInputCol("zyts").
      setOutputCol("bucketizer_feature")

    val fact_test_sflb_zyts_df2 = bucketizer.
      transform(fact_test_sflb_zyts_df1)
    val inputCols = fact_test_sflb_zyts_df1.
      columns.filter(_ != "lsh").filter(_ != "zyts")
    val vecDF = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("features").
      transform(fact_test_sflb_zyts_df2)

    val model = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("bucketizer_feature").
      setFeaturesCol("features").
      setPredictionCol("prediction").
      fit(vecDF)

    val FACT_TEST_FACTOR_TR = model.featureImportances.toArray.zip(inputCols)
    val FACT_TEST_FACTOR_TR_DF = spark.sparkContext.parallelize(FACT_TEST_FACTOR_TR).toDF("importance","index")
    FACT_TEST_FACTOR_TR_DF.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_facor_res_tr")
  }
}
