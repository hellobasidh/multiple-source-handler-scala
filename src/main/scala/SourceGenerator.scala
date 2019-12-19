import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SourceGenerator {

  def getSourceDataFrame(spark: SparkSession, sourceRulesetDF :Dataset[Row]) : Dataset[Row] =
  {
    try
      {
        //Load the source data into dataframes
        val path = getClass.getResource("JsonSource_1.json")
        val sourceDF = spark.read.json(path.getPath)

        //processing the source data
        val resultDF = getResultDataFrame(spark,sourceDF,sourceRulesetDF)
        resultDF.show()
        //final processed source data
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
        //retriving the rules for processing the source data
        val columnName = rulesList.get(0).getString(1)
        val condition = rulesList.get(0).getString(2)
        val value = rulesList.get(0).getString(4)
        import  spark.implicits._
        //filtering the data based on the ruleset
        val resultDF = sourceDF.filter(s"$columnName $condition '$value'" )
        resultDF.show()
        //processed dataframe is returned
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
