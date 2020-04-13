package c_pressess1

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable

object FACT_TEST_LDA2_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_lda_df0 = spark.sql("select lsh,djtsy,sjmlmc from clinical_path.fact_test_lda_data_2")
    val separator = "`"
    val fact_test_lda_df01=fact_test_lda_df0.select(concat_ws(separator, $"lsh", $"djtsy").cast(StringType).as("value"),$"sjmlmc").cache()

      //对第几天的项目进行组合
      val fact_test_lda_df1 = fact_test_lda_df01.map(v => {
        val lsh = v.getAs[String]("value")
        val sjmlmc = v.getAs[String]("sjmlmc")
        (lsh, (sjmlmc))
      }).rdd.map(v => (v._1, Array(v._2))).reduceByKey(_ ++ _).toDF("lsh", "sjmlmc")
      //对组合的项目算相应的频数
      val cv = new CountVectorizer().setInputCol("sjmlmc").setOutputCol("features")
      val fact_test_lda_df_model = cv.fit(fact_test_lda_df1)
      val fact_test_lda_df2 = fact_test_lda_df_model.transform(fact_test_lda_df1)
      /** 获得转成向量时词表 */
      //项目名称和其序号
      val vocabulary = spark.sparkContext.parallelize(fact_test_lda_df_model.vocabulary.zipWithIndex).
        toDF("xmmc","index")
      //建立模型，主题设置为7个
      val lda = new LDA().setK(4).setMaxIter(20).setOptimizer("em").setDocConcentration(2.2).setTopicConcentration(1.5)
      val ldamodel = lda.fit(fact_test_lda_df2)

      val schema = StructType(List(StructField("topics", IntegerType, false),
        StructField("xm", MapType(IntegerType, DoubleType), false)))
      /** ****************************************************/
      //得到词与主题的频率关系
      val topics = ldamodel.describeTopics(20)

      val topics1 = topics.rdd.map(row => {
        val array1 = row.get(1).asInstanceOf[mutable.WrappedArray[Integer]]
        val array2 = row.get(2).asInstanceOf[mutable.WrappedArray[DoubleType]]
        Row(row.getAs[IntegerType](0), (array1.array.zip(array2.array)).toMap)
      })

      val topics1_df = spark.createDataFrame(topics1, schema)
      val topics2 = topics1_df.select($"topics", explode(col("xm"))).toDF("topics", "key", "value");
      val dic_dic_mxfl_df = spark.sql(
        """
          |SELECT distinct sjmlmc,sdmlbz FROM clinical_path.dic_dic_mxfl
        """.stripMargin)
      val fact_test_lda_topic = topics2.selectExpr("topics+1 as topic",
        "key as key",
        "round(value,4) as termWeights").join(vocabulary, $"index" === $"key", "inner").join(dic_dic_mxfl_df, $"xmmc" === $"sjmlmc", "inner").select(
        "topic", "xmmc", "sdmlbz", "termWeights")

      /******************************************************/
      //得到第几天使用与主题的关系
      val ladmodel = ldamodel.transform(fact_test_lda_df2)
      val ladmodel1 = ladmodel.selectExpr("lsh", "topicDistribution as topicDistribution").
        rdd.map(row => {
        val array = row.get(1).asInstanceOf[DenseVector].toArray
        (row.get(0).asInstanceOf[String],
          array.zipWithIndex.maxBy(_._1)._2 + 1,
          array(0),
          array(1),
          array(2),
          array(3)/*,
          array(4),
          array(5),
          array(6)*/)
      })
      val fact_test_lda_ladmodel = ladmodel1.toDF("lsh","topic","topicDistribution1", "topicDistribution2", "topicDistribution3",
        "topicDistribution4"/*, "topicDistribution5", "topicDistribution6", "topicDistribution7"*/)
        .withColumn("splitcol",split(col("lsh"), "`")).select(
        col("splitcol").getItem(0).as("lsh"),
        col("splitcol").getItem(1).as("djtsy"),
        $"topic",$"topicDistribution1", $"topicDistribution2", $"topicDistribution3",
        $"topicDistribution4"/*, $"topicDistribution5", $"topicDistribution6", $"topicDistribution7"*/)
    //保存数据到hive中
    fact_test_lda_topic.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_lda_topic_2_1")
    fact_test_lda_ladmodel.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_lda_ladmodel_2_1")
  }
}
