import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._

object CSVFromDatabase {
  val usage = """
      Usage: --ouput [URI] --db [JDBC-URI] --pid [PROJECT_ID]
  """

  def optionParse(optMap: Map[String,String], arglist: List[String]) : Map[String,String] = {
    arglist match {
      case Nil => optMap
      case "--output" :: opt :: tail => optionParse(optMap ++ Map("outfile" -> opt), tail)
      case "--db" :: opt :: tail => optionParse(optMap ++ Map("database" -> opt), tail)
      case "--pid" :: opt :: tail => optionParse(optMap ++ Map("projectID" -> opt), tail)
      case str :: tail => println(usage); exit(1)
    }
  }

  def main(args: Array[String]) {
    if (args.length != 6) {
      println(usage)
      exit(1)
    }

    val optionMap = optionParse(Map(), args.toList)

    val conf = new SparkConf().setAppName("Warehouse from Panoptes")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val pid = optionMap.get("projectID") match {
      case Some(pid) => Some(pid.toInt)
      case None => None
    }

    DatabaseLoad.load(sqlContext, optionMap.getOrElse("database", ""), pid) match {
      case Vector(cs, md, as) => CSVExport.export(sqlContext, cs, as, md, optionMap.getOrElse("outfile", "out.csv"), pid.get)
      case _ => println("Something went wrong")
    }
  }
}
