import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SourceGenerator {

  def getSourceDataFrame(spark: SparkSession, sourceRulesetDF :Dataset[Row]) : Dataset[Row] =
  {
    try
      {
        val path = getClass.getResource("JsonSource_1.json")
        val sourceDF = spark.read.json(path.getPath)

        val resultDF = getResultDataFrame(spark,sourceDF,sourceRulesetDF)
        resultDF.show()
        resultDF
      }
    catch
      {
        case e: Exception => {
          println(e.printStackTrace())
          throw e
        }
      }

  }
  def getResultDataFrame(spark: SparkSession, sourceDF :Dataset[Row],sourceRulesetDF :Dataset[Row]) : Dataset[Row] =
  {
    try
      {
        val rulesList = sourceRulesetDF.collectAsList()
        val columnName = rulesList.get(0).getString(1)
        val condition = rulesList.get(0).getString(2)
        val value = rulesList.get(0).getString(4)
        import  spark.implicits._
        val resultDF = sourceDF.filter(s"$columnName $condition '$value'" )
        resultDF.show()
        resultDF
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
