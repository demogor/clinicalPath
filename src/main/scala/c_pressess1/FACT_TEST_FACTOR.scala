package c_pressess1


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Bucketizer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

object FACT_TEST_FACTOR {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_sflb_zyts_df = spark.sql("select * from clinical_path.fact_test_sflb_zyts limit 100").withColumn("Hos_class", $"Hos_class".cast(DataTypes.DoubleType))
    //列转行
    val fact_test_sflb_zyts_df1 = fact_test_sflb_zyts_df.groupBy($"lsh", $"zyts", $"Hos_class", $"num_patients").pivot("fylb", Seq("C", "S", "X", "Y", "Z", "F")).sum("fyze").na.fill(0)
    //因变量处理
    val splits = Array(0, 5, Double.PositiveInfinity)
    val bucketizer = new Bucketizer().setSplits(splits).setInputCol("zyts").setOutputCol("bucketizer_feature")
    val fact_test_sflb_zyts_df2 = bucketizer.transform(fact_test_sflb_zyts_df1)
    //选取自变量
    val colArray2 = Array("Hos_class", "num_patients", "C")
    //建模
    val vecDF = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(fact_test_sflb_zyts_df2)
    //val lr = new LogisticRegression().setMaxIter(10).setLowerBoundsOnCoefficients(new DenseMatrix(1, 3, Array(0.0,0.0,0.0)))
    val lr = new LogisticRegression().setMaxIter(10)

    val lrModel = lr.setLabelCol("bucketizer_feature").setFeaturesCol("features").fit(vecDF)
    //查看预测的
    println(lrModel.transform(vecDF).show)
    //查看系数
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
  }
}
