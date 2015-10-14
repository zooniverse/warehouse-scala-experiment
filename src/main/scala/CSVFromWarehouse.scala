import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext}
import com.databricks.spark.avro._

object CSVFromWarehouse {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CSV from Warehouse")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val classifications = sqlContext.read.avro("classifications_output/")
    val annotations = sqlContext.read.avro("annotations_output/")
    val metadata = sqlContext.read.avro("metadata_output/")

    CSVExport.export(sqlContext, classifications, annotations, metadata, "out.csv", 3)
  }
}

