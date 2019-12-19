import org.apache.spark.sql.{Dataset, Row, SparkSession}

object RulesetHandler {

  def getRulesets(spark: SparkSession, sourceId: String) : Dataset[Row] =
  {
    try
      {
        val getRes = getClass.getResource("Ruleset.json")
        //val path = "D:/Basidh/SampleJsonData/Ruleset.json"
        val rulesetDF = spark.read.json(getRes.getPath)
        rulesetDF.show()
        import  spark.implicits._
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
