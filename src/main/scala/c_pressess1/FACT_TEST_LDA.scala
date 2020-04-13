package c_pressess1


import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions._
import scala.collection.mutable


object FACT_TEST_LDA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val fact_test_lda_df = spark.sql("select djtsy,sjmlmc from clinical_path.fact_test_lda_data")

    //对第几天的项目进行组合
    val fact_test_lda_df1 = fact_test_lda_df.map(v => {
      val djtsy = v.getAs[Int]("djtsy")
      val sjmlmc = v.getAs[String]("sjmlmc")
      (djtsy, (sjmlmc))
    }).rdd.map(v => (v._1, Array(v._2))).reduceByKey(_ ++ _).toDF("djtsy", "sjmlmc").cache()
    //对组合的项目算相应的频数
    val cv = new CountVectorizer().setInputCol("sjmlmc").setOutputCol("features")
    val fact_test_lda_df_model = cv.fit(fact_test_lda_df1)
    val fact_test_lda_df2 = fact_test_lda_df_model.transform(fact_test_lda_df1)
    /** 获得转成向量时词表 */
    val vocabulary = fact_test_lda_df_model.vocabulary


    //建立模型，主题设置为3个
    val lda = new LDA()
      .setK(5)
      .setMaxIter(20)
      .setOptimizer("em")
      .setDocConcentration(2.2)
      .setTopicConcentration(1.5)
    val ldamodel = lda.fit(fact_test_lda_df2)

    val shema = StructType(List(StructField("djtsy", IntegerType, false),
      StructField("xm", MapType(IntegerType, DoubleType), false)))
    //得到词与主题的频率关系
    val topics = ldamodel.describeTopics(20)
    val topics1 = topics.rdd.map(row => {
      val array1 = row.get(1).asInstanceOf[mutable.WrappedArray[Integer]]
      val array2 = row.get(2).asInstanceOf[mutable.WrappedArray[DoubleType]]
      Row(row.getAs[IntegerType](0), (array1.array.zip(array2.array)).toMap)
    })
    val topics1_df = spark.createDataFrame(topics1, shema)
    val topics2 = topics1_df.select($"djtsy", explode(col("xm"))).toDF("topics", "key", "value");

    def XMZH(arr: Int) = {
      vocabulary(arr)
    }

    val my2_udf = udf(XMZH _)
    val dic_dic_mxfl_df = spark.sql(
      """
        |SELECT distinct sjmlmc,sdmlbz FROM clinical_path.dic_dic_mxfl
      """.stripMargin)
    val fact_test_lda_topic = topics2.withColumn("term", my2_udf(col("key"))).selectExpr("topics+1 as topic",
      "term as term",
      "round(value,4) as termWeights").join(dic_dic_mxfl_df, $"term" === $"sjmlmc", "inner").select(
      "topic", "term", "sdmlbz", "termWeights")
    fact_test_lda_topic.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_lda_topic")

    //得到第几天使用与主题的关系
    val ladmodel = ldamodel.transform(fact_test_lda_df2)
    val ladmodel1 = ladmodel.selectExpr("djtsy", "topicDistribution as topicDistribution").
      rdd.map(row => {
      val array = row.get(1).asInstanceOf[DenseVector].toArray
      (row.get(0).asInstanceOf[Int],
        array.zipWithIndex.maxBy(_._1)._2 + 1,
        array(0),
        array(1),
        array(2),
        array(3),
        array(4))
      //        array(5),
      //        array(6))
    })
    val fact_test_lda_ladmodel = ladmodel1.toDF("djtsy", "topic", "topicDistribution1", "topicDistribution2", "topicDistribution3"
      ,"topicDistribution4","topicDistribution5"
      //,"topicDistribution6","topicDistribution7"
    )
    fact_test_lda_ladmodel.write.mode("Overwrite").saveAsTable("clinical_path.fact_test_lda_ladmodel")
  }
}
