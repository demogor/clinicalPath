package c_pressess1

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.DataFrame

class LDAThemeAnalysis {
  def data(spark: SparkSession): DataFrame ={
    import spark.implicits._
    val data = spark.sparkContext.parallelize(Seq(
      (1,Array("祖国","万岁")),
      (2,Array("中华人民共和国","雄起")),
      (3,Array("万岁","中国")),
      (4,Array("祖国","雄起")),
      (5,Array("中华","雄起")),
      (6,Array("雄起")))).map{x =>
      (x._1,x._2)
    }.toDF("id","context")
    data
  }

  def valueCompute(spark: SparkSession,dataFrame: DataFrame): Unit ={
    import spark.implicits._
    val cv = new CountVectorizer().setInputCol("context").setOutputCol("features")
    val cvmodel = cv.fit(dataFrame)
    val cvResult: DataFrame = cvmodel.transform(dataFrame)
    /**获得转成向量时词表*/
    val vocabulary = cvmodel.vocabulary

    /**setK:主题（聚类中心）个数
      * setMaxIter：最大迭代次数
      * setOptimizer:优化计算方法，支持”em“和”online“
      * setDocConcentration：文档-主题分布的堆成先验Dirichlet参数，值越大，分布越平滑，值>1.0
    * setTopicConcentration:文档-词语分布的先验Dirichlet参数，值越大，分布越平滑，值>1.0
    *setCheckpointInterval:checkpoint的检查间隔
    * */
    val lda = new LDA()
      .setK(3)
      .setMaxIter(20)
      .setOptimizer("em")
      .setDocConcentration(2.2)
      .setTopicConcentration(1.5)

    val ldamodel: LDAModel = lda.fit(cvResult)
    /**可能度*/
    ldamodel.logLikelihood(cvResult)
    /**困惑度，困惑度越小，模型训练越好*/
    ldamodel.logPerplexity(cvResult)

    val ladmodel: DataFrame = ldamodel.transform(cvResult)
    val topic =ladmodel.select($"topicDistribution").show
    println(ladmodel.show)
  }
}


object FACT_TEST_LDA_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      //.master("spark://cm01.spark.com:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .appName("clinicalpath")
      .enableHiveSupport()
      .getOrCreate()

    val lDAThemeAnalysis = new LDAThemeAnalysis
    val dataFrame = lDAThemeAnalysis.data(spark)
    lDAThemeAnalysis.valueCompute(spark,dataFrame)
    println("。。。。。。。。。 还没有整好 。。。。。。。。。")
  }
}