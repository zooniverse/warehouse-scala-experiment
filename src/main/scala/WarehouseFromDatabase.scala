import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._

object WarehouseFromDatabase {
  def sqlString(multilineString: String) : String = {
    multilineString.stripMargin.replaceAll("\n", " ")
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

    classifications
      .withColumn("subject_ids", split(classifications("subject_ids"), ","))
      .drop("annotations")
      .drop("metadata")
      .write.options(Map(
                       "recordName" -> "Classifications",
                       "recordNamespace" -> "org.zooniverse.avro"
                     ))
      .partitionBy("project_id", "workflod_id")
      .avro("output")
  }
}
