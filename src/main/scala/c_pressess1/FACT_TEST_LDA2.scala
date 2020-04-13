package c_pressess1

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import scala.collection.mutable

object FACT_TEST_LDA2 {
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
    val schemaTopsis = StructType(List(StructField("topics", IntegerType, false),
      StructField("djtsy", IntegerType, false),
      StructField("xmmc", StringType,false),
        StructField("sdmlbz",  StringType, false),
      StructField("termWeights",  DoubleType, false)))


    val schemaladmodel = StructType(List(StructField("lsh", StringType, false),
      StructField("topic", IntegerType, false),
      StructField("topicDistribution1", DoubleType, false),
      StructField("topicDistribution2", DoubleType, false),
      StructField("topicDistribution3", DoubleType, false),
      StructField("topicDistribution4", DoubleType, false),
      StructField("topicDistribution5", DoubleType, false),
      StructField("topicDistribution6", DoubleType, false),
      StructField("topicDistribution7", DoubleType, false),
        StructField("djtsy", IntegerType, false)))
    var fact_test_lda_topic_2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaTopsis)
    var fact_test_lda_ladmodel_2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaladmodel)

    for (l <- 1 to 12) {
      //对第几天的项目进行组合
      val fact_test_lda_df = fact_test_lda_df0.filter($"djtsy" === l).cache()
      val fact_test_lda_df1 = fact_test_lda_df.map(v => {
        val lsh = v.getAs[String]("lsh")
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
      val lda = new LDA()
        .setK(7)
        .setMaxIter(20)
        .setOptimizer("em")
        .setDocConcentration(2.2)
        .setTopicConcentration(1.5)
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
      //val topics2 = topics1_df.select($"topics", $"djtsy", explode(col("xm"))).toDF("topics", "djtsy", "key", "value");
      val topics2 = topics1_df.select($"topics", ($"topics"*0)+l, explode(col("xm"))).toDF("topics", "djtsy", "key", "value");
      val dic_dic_mxfl_df = spark.sql(
        """
          |SELECT distinct sjmlmc,sdmlbz FROM clinical_path.dic_dic_mxfl
        """.stripMargin)
      val fact_test_lda_topic = topics2.selectExpr("topics+1 as topic",
        "djtsy as djtsy",
        "key as key",
        "round(value,4) as termWeights").join(vocabulary, $"index" === $"key", "inner").join(dic_dic_mxfl_df, $"xmmc" === $"sjmlmc", "inner").select(
        "topic","djtsy", "xmmc", "sdmlbz", "termWeights")
      fact_test_lda_topic_2 = fact_test_lda_topic_2.union(fact_test_lda_topic)

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
          array(3),
          array(4),
          array(5),
          array(6))
      })
      val fact_test_lda_ladmodel = ladmodel1.toDF("lsh","topic","topicDistribution1", "topicDistribution2", "topicDistribution3",
        "topicDistribution4", "topicDistribution5", "topicDistribution6", "topicDistribution7").withColumn("djtsy",$"topic"*0+l)
      fact_test_lda_ladmodel_2 = fact_test_lda_ladmodel_2.union(fact_test_lda_ladmodel)
    }
    fact_test_lda_topic_2.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_lda_topic_2")
    fact_test_lda_ladmodel_2.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_lda_ladmodel_2")
  }
}
