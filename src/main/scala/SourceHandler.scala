import org.apache.spark.sql.SparkSession

object SourceHandler {

  def main(args: Array[String]): Unit = {

    try
      {
        println("Notebook Sample Program")

        //Retriving the Source Id for loading the ruleset
        val sourceId = args(0)
        //Initialize Spark session
        val spark = SparkSession
          .builder
          .appName("Source Builder")
          .config("spark.master", "local")
          .getOrCreate()
        //Loading the ruleset into a dataframe
        val sourceRuleDF = RulesetHandler.getRulesets(spark,sourceId)
        //processing the source data based on the loaded ruleset
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
