import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import com.databricks.spark.csv._
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import java.net.URI
import spray.json._
import DefaultJsonProtocol._

object CSVFromWarehouse {
  def copyMergeWithHeader(srcFS: FileSystem, srcPath: Path, dstFS: FileSystem, destPath: Path, deleteOriginal: Boolean, hadoopConfig: Configuration, headers: Option[String]) = {
    val out = dstFS.create(destPath)

    headers match {
      case Some(header) => out.write((header + "\n").getBytes("UTF-8"))
      case _ => None
    }

    try {
      for( content <- srcFS.listStatus(srcPath) ) {
        val in = srcFS.open(content.getPath())
        try {
          IOUtils.copyBytes(in, out, hadoopConfig, false);
        } finally {
          in.close()
        }
      }
    } finally {
      out.close();
    }

    if (deleteOriginal) srcFS.delete(srcPath, true) else true
  }

  def merge(srcPath: String, destPath: String, headers: String, deleteOriginal: Boolean = false) {
    val hadoopConfig = new Configuration()
    val srcFs = FileSystem.get(URI.create(srcPath), hadoopConfig)
    val destFs = FileSystem.get(URI.create(destPath), hadoopConfig)
    copyMergeWithHeader(srcFs, new Path(srcPath), destFs, new Path(destPath), deleteOriginal, hadoopConfig, Some(headers))
  }

  def dimensions(dimension: String, dimensions: Seq[Map[String,Int]]) : Option[Int] = Option(dimensions) match {
    case Some(Seq(dimMap: Map[String, Int], _*)) => dimMap.get(dimension)
    case Some(Seq(dimMap: Map[String, Int])) => dimMap.get(dimension)
    case None => None
    case _ => None
  }

  def seqToString = udf { (seq: Seq[Any]) => Option(seq) match {
                           case Some(seq) => Some(seq.mkString(","))
                           case None => None
                         }}

  def writeCSV(df: DataFrame, projectID: Int) {
    val toWrite = df.filter(df("project_id") === projectID)
    val inPath = "./project_" + projectID.toString() + "_partitioned.csv"
    val outPath = "./project_" + projectID.toString() + ".csv"


    toWrite.write.partitionBy("project_id")
      .format("com.databricks.spark.csv").save(inPath)
    merge(inPath, outPath, fields, true)
  }

  def parseStrings = udf { strings: String => strings.parseJson.convertTo[Map[String,String]] }

  def extractStrings(postfix: String, strings: Map[String, String], task: String, index: Option[Int]) = {
    val prefix = task + "." + postfix
    val key = index match {
      case Some(i) => prefix + "." + i + ".label"
      case None => prefix
    }
    strings.get(key).getOrElse("").stripMargin.replaceAll("\n", " ")
  }

  def fields = "id,user_id,updated_at,created_at,user_group_id,completed,gold_standard,expert_classifier,workflow_version,subject_ids,user_ip,answered_tasks,project_id,workflow_id,task,value,choice,answers,filters,marking,frame,tool,details,started_at,finished_at,user_agent,utc_offset,user_language,task_label,tool_label,value_label,viewport_height,viewport_width,client_height,client_width,natural_height,natural_width"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CSV from Warehouse")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val workflowsQuery = "(SELECT w.id as wid, wc.strings::TEXT FROM workflows w JOIN workflow_contents wc ON wc.workflow_id = w.id WHERE wc.language = w.primary_language) as wq"

    val classifications = sqlContext.read.avro("classifications_output/")
    val annotations = sqlContext.read.avro("annotations_output/")
    val metadata = sqlContext.read.avro("metadata_output/")
    val workflows = sqlContext.read.format("jdbc")
      .options(Map(
                 "url" -> "jdbc:postgresql://192.168.99.100:5432/panoptes_development?user=panoptes&password=panoptes",
                 "dbtable" -> workflowsQuery,
                 "driver" -> "org.postgresql.Driver"
               ))
      .load

    val getClientHeight = udf { column: Seq[Map[String,Int]] => dimensions("clientHeight", column) }
    val getClientWidth = udf { column: Seq[Map[String,Int]] => dimensions("clientWidth", column) }
    val getNaturalHeight = udf { column: Seq[Map[String,Int]] => dimensions("naturalHeight", column) }
    val getNaturalWidth = udf { column: Seq[Map[String,Int]] => dimensions("naturalWidth", column) }

    val getTool = udf { (strings: Map[String,String], task: String, index: Int) => {
                         extractStrings("tools", strings, task, Option(index))
                       }}

    val getQuestion = udf { (strings: Map[String,String], task: String) => {
                             extractStrings("question", strings, task, None)
                           }}

    val getAnswer = udf { (strings: Map[String,String], task: String, index: Int) => {
                           extractStrings("answers", strings, task, Option(index))
                         }}

    val joined = classifications
      .join(annotations.drop("project_id").drop("workflow_id"), "id")
      .join(metadata.drop("project_id").drop("workflow_id"), "id")
      .join(workflows.withColumn("strings", parseStrings(workflows("strings"))),
            classifications("workflow_id") === workflows("wid"))

    val withWorkflowInfo = joined
      .withColumn("task_label", getQuestion(joined("strings"), joined("task")))
      .withColumn("tool_label", getTool(joined("strings"), joined("task"), joined("tool")))
      .withColumn("value_label", getAnswer(joined("strings"), joined("task"), joined("value")))

    val exploded = withWorkflowInfo.withColumn("viewport_height", joined("viewport.height"))
      .withColumn("viewport_width", joined("viewport.width"))
      .withColumn("client_height", getClientHeight(joined("subject_dimensions")))
      .withColumn("client_width", getClientWidth(joined("subject_dimensions")))
      .withColumn("natural_height", getNaturalHeight(joined("subject_dimensions")))
      .withColumn("natural_width", getNaturalWidth(joined("subject_dimensions")))
      .withColumn("marking", seqToString(joined("marking")))
      .withColumn("details", seqToString(joined("details")))
      .withColumn("subject_ids", seqToString(joined("subject_ids")))
      .withColumn("answered_tasks", seqToString(joined("answered_tasks")))

    writeCSV(exploded.select(fields.split(",").map(exploded(_)) : _*), 3)
  }
}

