import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import com.databricks.spark.csv._

object CSVFromWarehouse {
  def dimensions(dimension: String, dimensions: Seq[Map[String,Int]]) : Option[Int] = Option(dimensions) match {
    case Some(Seq(dimMap, _*)) => dimMap.get(dimension)
    case Some(Seq(dimMap)) => dimMap.get(dimension)
    case None => None
  }

  def seqToString = udf { (seq: Seq[Any]) => Option(seq) match {
                           case Some(seq) => Some(seq.mkString(","))
                           case None => None
                         }}

  def writeCSV(df: DataFrame, projectID: Option[Int]) {
    val toWrite = projectID match {
      case Some(pid) => df.filter(df("project_id") === pid)
      case None => df
    }

    toWrite.write.partitionBy("project_id")
      .format("com.databricks.spark.csv").option("header", "true").save("projects.csv")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CSV from Warehouse")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val classifications = sqlContext.read.avro("classifications_output/")
    val annotations = sqlContext.read.avro("annotations_output/")
    val metadata = sqlContext.read.avro("metadata_output/")

    val getClientHeight = udf { column: Seq[Map[String,Int]] => dimensions("clientHeight", column) }
    val getClientWidth = udf { column: Seq[Map[String,Int]] => dimensions("clientWidth", column) }
    val getNaturalHeight = udf { column: Seq[Map[String,Int]] => dimensions("naturalHeight", column) }
    val getNaturalWidth = udf { column: Seq[Map[String,Int]] => dimensions("naturalWidth", column) }

    val joined = classifications
      .join(annotations.drop("project_id").drop("workflow_id"), "id")
      .join(metadata.drop("project_id").drop("workflow_id"), "id")

    writeCSV(joined.withColumn("viewport_height", joined("viewport.height"))
               .withColumn("viewport_width", joined("viewport.width"))
               .withColumn("client_height", getClientHeight(joined("subject_dimensions")))
               .withColumn("client_width", getClientWidth(joined("subject_dimensions")))
               .withColumn("natural_height", getNaturalHeight(joined("subject_dimensions")))
               .withColumn("natural_width", getNaturalWidth(joined("subject_dimensions")))
               .withColumn("marking", seqToString(joined("marking")))
               .withColumn("details", seqToString(joined("details")))
               .withColumn("subject_ids", seqToString(joined("subject_ids")))
               .withColumn("answered_tasks", seqToString(joined("answered_tasks")))
               .drop("viewport")
               .drop("subject_dimensions"), None)
  }
}

