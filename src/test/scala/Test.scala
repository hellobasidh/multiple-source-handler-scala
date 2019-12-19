object Test extends App {

  val sourceHandlerObj = SourceHandler
  //kindly set the winUtils path from your local directory here
  System.setProperty("hadoop.home.dir", "D:\\Avik\\winutils");
  //input the ruleset source id for processing the data
  sourceHandlerObj.main(Array("3"))

}