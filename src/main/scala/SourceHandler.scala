import org.apache.spark.sql.SparkSession

object SourceHandler {

  def main(args: Array[String]): Unit = {

    try
      {
        println("Notebook Sample Program")
        val sourceId = args(0)
        val spark = SparkSession
          .builder
          .appName("Source Builder")
          .config("spark.master", "local")
          .getOrCreate()
        val sourceRuleDF = RulesetHandler.getRulesets(spark,sourceId)

        SourceGenerator.getSourceDataFrame(spark,sourceRuleDF)
      }
    catch
      {
        case e: Exception => {
          println(e.printStackTrace())
          throw e
        }
      }

  }

}
