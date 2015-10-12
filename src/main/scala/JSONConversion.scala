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

case class Classification(id: Int, user_id: Option[Int], updated_at: Int, created_at: Int,
                          user_group_id: Option[Int], completed: Boolean, gold_standard: Option[Boolean],
                          expert_classifier: Int, workflow_version: String, subject_ids: Array[Int],
                          user_ip: String, project_id: Int, workflow_id: Int, annotations: Vector[Annotation],
                          metadata: Metadata)

object ClassificationJsonProtocol extends DefaultJsonProtocol {
  implicit val classificationFormat = jsonFormat15(Classification)
}
