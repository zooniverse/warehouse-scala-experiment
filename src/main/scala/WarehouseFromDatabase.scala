import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._

object WarehouseFromDatabase {
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

    val dbs = DatabaseLoad.load(sqlContext, "jdbc:postgresql://192.168.99.100:5432/panoptes_development?user=panoptes&password=panoptes", Some(3))

    dbs match {
      case Vector(cs, md, as) => {
        writeAvro(cs, "Classification", "classifications_output")
        writeAvro(md, "ClassificationMetadata", "metadata_output")
        writeAvro(as, "ClassificationAnnotation", "annotations_output")
      }

      case _ => println("Something went wrong")
    }
  }
}
