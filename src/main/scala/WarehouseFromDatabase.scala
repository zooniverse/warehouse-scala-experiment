import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import spray.json._

case class Metadata(viewport: Option[Map[String, Int]], started_at: Option[String], finished_at: Option[String], user_agent: Option[String], utc_offset: Option[String], user_language: Option[String], subject_dimensions: Option[List[Option[Map[String,Int]]]])

object MetadataJsonProtocol extends DefaultJsonProtocol {
  implicit val metadataFormat = jsonFormat7(Metadata)
}

import MetadataJsonProtocol._

case class Annotation(task: String, value: String, tool: Option[Integer], details: Option[List[Details]], filters: Option[Map[String,String]])

object AnnotationJsonProtocol extends DefaultJsonProtocol {
  implicit object AnnotationJsonFormat extends RootJsonFormat[Annotation] {
    def read(value: JsValue) = {
      value.asJsObject.getFields("task", "value", "f") match {
        case Seq(JsString(task), JsString(value)) => Annotation(task, value, None, None)
        case Seq(JsString(task), JsObject(value)) => {
          value.getFields("choise an")
        }
        case Seq(JsString(task), JsArray(value)) = 
    }
}

object WarehouseFromDatabase {
  def sqlString(multilineString: String) : String = {
    multilineString.stripMargin.replaceAll("\n", " ")
  }

  def writeAvro(df: DataFrame, recordName: String, outputLocation: String): Unit = {
    df.write.options(Map(
                     "recordName" -> recordName,
                     "recordNamespace" -> "org.zooniverse.avro"
                   ))
      .partitionBy("project_id", "workflow_id")
      .avro(outputLocation)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Warehouse from Panoptes")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
    sqlContext.setConf("spark.sql.avro.deflate.level", "5")

    val classificationsQuery = sqlString("""(SELECT id, project_id, user_id, workflow_id, updated_at,
                                 |created_at, user_group_id, completed, gold_standard, expert_classifier,
                                 |workflow_version, array_to_string(subject_ids, ',') as subject_ids,
                                 |metadata::TEXT, annotations::TEXT FROM classifications) as cs""")

    val classifications = sqlContext.read.format("jdbc")
      .options(Map(
                 "url" -> "jdbc:postgresql://192.168.99.100:5432/panoptes_development?user=panoptes&password=panoptes",
                 "dbtable" -> classificationsQuery,
                 "driver" -> "org.postgresql.Driver"
               ))
      .load

    val parseMetadata = udf { (metadata: String) => metadata.parseJson.convertTo[Metadata] }

    val cs = classifications
      .withColumn("subject_ids", split(classifications("subject_ids"), ","))
      .drop("annotations")
      .drop("metadata")

    val md = classifications
      .withColumn("metadata", parseMetadata(classifications("metadata")))
      .select("id", "project_id", "workflow_id", "metadata.viewport", "metadata.started_at", "metadata.finished_at",
              "metadata.user_agent", "metadata.utc_offset", "metadata.user_language", "metadata.subject_dimensions")

    val as = classifications

    writeAvro(cs, "Classification", "classifications_output")
    writeAvro(md, "ClassificationMetadata", "metadata_output")
    writeAvro(as, "ClassificationAnnotation", "annotations_output")
  }
}
