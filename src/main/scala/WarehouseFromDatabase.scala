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

//abstract class SurveyAnswer extends Product
//case class SingleAnswer(answer: String) extends SurveyAnswer
//case class MultiAnswer(answer: Vector[String]) extends SurveyAnswer

//object SurveyAnswerJsonProtocol extends DefaultJsonProtocol {
//implicit object SurveyAnswerJsonFormat extends RootJsonFormat[SurveyAnswer] {
//def read(value: JsValue) = value match {
//case JsString(answer) => SingleAnswer(answer)
//case JsArray(answer) => MultiAnswer(answer.map(_.convertTo[String]))
//case _ => deserializationError("Incorrect Format for Survey Task")
//}
//
//def write(value: SurveyAnswer) = value match {
//case SingleAnswer(value) => JsString(value)
//case MultiAnswer(value) => JsArray(value.map(JsString(_)))
//}
//}
//}

//import SurveyAnswerJsonProtocol._

// This is an ugly case class because Spark doesn't support Union types yet, it should eventually be refactored into:
// abstract class Annotation
// case class SimpleAnnotation(task: String, value: Int) extends Annotation
// case class SurveyAnnotation(task: String, choice: String, answers: Map[String, SurveyAnswer], filters: Map[String, String]) extends Annotation
// case class DrawingAnnotation(task: String, marking: Vector[(Double, Double)], frame: Int, tool: Int, details: Vector[Int]) extends Annotation

case class Annotation(task: String, value: Option[Int],
                      choice: Option[String], answers: Option[Map[String, Vector[String]]], filters: Option[Map[String, String]],
                      marking: Option[Vector[(Double, Double)]], frame: Option[Int], tool: Option[Int], details: Option[Vector[Int]])

object AnnotationJsonProtocol extends DefaultJsonProtocol {
  def simpleAnnotation(task: String, value: Int) = {
    Annotation(task, Some(value), None, None, None, None, None, None, None)
  }

  def surveyAnnotation(task: String, choice: String, answers: Map[String, Vector[String]], filters: Map[String, String]) = {
    Annotation(task, None, Some(choice), Some(answers), Some(filters), None, None, None, None)
  }

  def drawingAnnotation(task: String, marking: Vector[(Double, Double)], frame: Int, tool: Int, details: Vector[Int]) = {
    Annotation(task, None, None, None, None, Some(marking), Some(tool), Some(frame), Some(details))
  }

  def readAnnotation(taskValue: Seq[JsValue]) : Seq[Annotation] = taskValue match {
    case Seq(JsString(task), JsNumber(value)) => Seq(simpleAnnotation(task, value.intValue()))
    case Seq(JsString(task), JsObject(value)) => {
      JsObject(value).getFields("choice", "answers", "filters") match {
        case Seq(JsString(choice), JsObject(answers), filters) => {
          val ans = answers.mapValues(_ match {
                                        case JsString(answer) => Vector(answer)
                                        case JsArray(answer) => answer.map(_.convertTo[String])
                                        case _ => deserializationError("Incorrect Format for Survey Task")
                                      })
          Seq(surveyAnnotation(task, choice, ans, filters.convertTo[Map[String,String]]))
        }
        case _ => deserializationError("Incorrect Format for Survey Task")
      }
    }
    case Seq(JsString(task), JsArray(values)) => {
      values.flatMap(
        (value: JsValue) => value match {
          case JsNumber(value) => Seq(simpleAnnotation(task, value.intValue()))
          case JsObject(value) => {
            value.get("choice") match {
              case Some(_) => readAnnotation(Seq(JsString(task), JsObject(value)))
              case None =>  {
                val tool = value.get("tool").get.convertTo[Int]
                val frame = value.get("frame").get.convertTo[Int]
                val details = value.get("details").get.convertTo[Vector[Int]]
                val points = value.filterKeys((key: String) => key != "tool" && key != "frame" && key != "details")
                  .groupBy[Char]((keyVal: (String, JsValue)) => keyVal._1.last)
                  .map((indexPoints: (Char, Map[String, JsValue])) => indexPoints match {
                         case (index, points) => {
                           println(index)
                           println(points)
                           (points.get("x" + index).get.convertTo[Double], points.get("y" + index).get.convertTo[Double])
                         }
                       })
                  .to[Vector]
                Seq(drawingAnnotation(task, points, frame, tool, details))
              }
            }
          }
          case _ => deserializationError("Incorrect Format for Annotation")
        })
    }
    case _ => deserializationError("Incorrect Format for Annotation")
  }

  implicit object AnnotationJsonFormat extends RootJsonFormat[Vector[Annotation]] {
    def read(annotations: JsValue) = annotations match {
      case JsArray(annotations) => {
        annotations.flatMap((annotation: JsValue) => readAnnotation(annotation.asJsObject.getFields("task", "value")))
      }
      case _ => deserializationError("Incorrect Format for Annotation")
    }

    def write(annotations: Vector[Annotation]) = JsArray(
      annotations.map(
        (annotation: Annotation) =>  annotation match {
          case Annotation(task, Some(value), _, _, _, _, _, _, _) => JsObject("task" -> JsString(task), "value" -> JsNumber(value))
          case _ => JsString("whoops")
        }))
  }
}

import AnnotationJsonProtocol._

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

  def parseMetadata = udf { (metadata: String) => metadata.parseJson.convertTo[Metadata] }
  def parseAnnotations = udf { (annotations: String) => annotations.parseJson.convertTo[Vector[Annotation]] }
  def taskAnswers = udf { (annotations: Seq[Annotation]) => annotations.map(_.task) }

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

    val parsedClassifications = classifications
      //      .filter(classifications("project_id") === 593)
      .withColumn("metadata", parseMetadata(classifications("metadata")))
      .withColumn("annotations", parseAnnotations(classifications("annotations")))
      .withColumn("subject_ids", split(classifications("subject_ids"), ","))

    val cs = parsedClassifications
      .withColumn("answered_tasks", taskAnswers(parsedClassifications("annotations")))
      .drop("annotations")
      .drop("metadata")

    val md = parsedClassifications
      .select("id", "project_id", "workflow_id", "metadata.*")

    val as = parsedClassifications
      .explode("annotations", "annotation"){ annotations: Seq[Annotation] => annotations }
      .select("id", "project_id", "workflow_id", "annotation")

    writeAvro(cs, "Classification", "classifications_output")
    writeAvro(md, "ClassificationMetadata", "metadata_output")
    writeAvro(as, "ClassificationAnnotation", "annotations_output")
    //   as.collect.foreach(println)
  }
}
