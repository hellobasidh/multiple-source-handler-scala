import org.apache.spark.sql.{Dataset, Row, SparkSession}

object RulesetHandler {


  def getRulesets(spark: SparkSession, sourceId: String) : Dataset[Row] =
  {
    try
      {
        //Load the ruleset file/table
        val getRes = getClass.getResource("Ruleset.json")
        //Load the ruleset in to Dataframe
        val rulesetDF = spark.read.json(getRes.getPath)
        rulesetDF.show()
        import  spark.implicits._
        //get the required ruleset based on the source id
        val sourceDF = rulesetDF.filter($"sourceId" === sourceId)
        sourceDF.show()
        sourceDF
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
