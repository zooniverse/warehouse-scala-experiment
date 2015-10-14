import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import spray.json._
import MetadataJsonProtocol._
import AnnotationJsonProtocol._

object DatabaseLoad {
  def sqlString(multilineString: String) : String = {
    multilineString.stripMargin.replaceAll("\n", " ")
  }

  def parseMetadata = udf { (metadata: String) => metadata.parseJson.convertTo[Metadata] }
  def parseAnnotations = udf { (annotations: String) => annotations.parseJson.convertTo[Vector[Annotation]] }
  def taskAnswers = udf { (annotations: Seq[Row]) => annotations.map(_.getString(0)) }

  def load(sqlContext: SQLContext, url: String) : Vector[DataFrame] = {
    val classificationsQuery = sqlString("""(SELECT id, project_id, user_id, workflow_id, updated_at,
                                 |created_at, user_group_id, completed, gold_standard, expert_classifier,
                                 |workflow_version, array_to_string(subject_ids, ',') as subject_ids,
                                 |metadata::TEXT, annotations::TEXT, user_ip FROM classifications) as cs""")

    val classifications = sqlContext.read.format("jdbc")
      .options(Map(
                 "url" -> url,
                 "dbtable" -> classificationsQuery,
                 "driver" -> "org.postgresql.Driver"
               ))
      .load

    val parsedClassifications = classifications
      .filter(classifications("project_id") === 3)
      .withColumn("metadata", parseMetadata(classifications("metadata")))
      .withColumn("annotations", parseAnnotations(classifications("annotations")))
      .withColumn("subject_ids", split(classifications("subject_ids"), ","))

    val cs = parsedClassifications
      .withColumn("answered_tasks", taskAnswers(parsedClassifications("annotations")))
      .drop("annotations")
      .drop("metadata")

    val md = parsedClassifications
      .select("id", "project_id", "workflow_id", "metadata.viewport", "metadata.started_at", "metadata.finished_at", "metadata.user_agent", "metadata.utc_offset",  "metadata.user_language", "metadata.subject_dimensions")

    val explodedAnnotations = parsedClassifications
      .withColumn("annotation", explode(parsedClassifications("annotations")))

    val as = explodedAnnotations
      .select("id", "project_id", "workflow_id", "annotation.task", "annotation.value", "annotation.choice", "annotation.answers", "annotation.filters", "annotation.marking", "annotation.frame", "annotation.tool", "annotation.details")

    Vector(cs, md, as)
  }
}
