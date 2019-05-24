package processing

import com.holdenkarau.spark.testing.{SharedSparkContext, StructuredStreamingBase}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.FunSuite

import scala.util.parsing.json.JSONObject

class SparkProcessingAppTest
  extends FunSuite with SharedSparkContext with StructuredStreamingBase {
  // re-use the spark context
  override implicit def reuseContextIfPossible: Boolean = true

  test("add 3") {
    import spark.implicits._
    val input = List(
      List(
        "{\"timestamp\":\"2019-05-22 18:00:00\", \"host\":\"h1\", \"level\":\"TRACE\", \"text\":\"trace\"}",
        "{\"timestamp\":\"2019-05-22 18:01:00\", \"host\":\"h1\", \"level\":\"INFO\", \"text\":\"info\"}",
        "{\"timestamp\":\"2019-05-22 18:00:00\", \"host\":\"h1\", \"level\":\"DEBUG\", \"text\":\"debug\"}",
        "{\"timestamp\":\"2019-05-22 18:00:00\", \"host\":\"h1\", \"level\":\"INFO\", \"text\":\"info\"}",
        "{\"timestamp\":\"2019-05-22 18:00:00\", \"host\":\"h1\", \"level\":\"TRACE\", \"text\":\"trace\"}",
        "{\"timestamp\":\"2019-05-22 18:00:00\", \"host\":\"h1\", \"level\":\"ERROR\", \"text\":\"error\"}",
        "{\"timestamp\":\"2019-05-22 18:00:00\", \"host\":\"h1\", \"level\":\"TRACE\", \"text\":\"trace\"}"
      )
    )
    val expected: Seq[String] = List(
    )

    def compute(input: DataFrame): Dataset[String] = {

      def convertRowToJSON(row: Row): String = {
        val m = row.getValuesMap(row.schema.fieldNames)
        JSONObject(m).toString()
      }

      SparkProcessingApp.windowedGroupByHostAndLevel(input).map(row => {
        val m = row.getValuesMap(row.schema.fieldNames)
        JSONObject(m).toString()
      })
    }

//    testSimpleStreamEndState(spark, input, expected, "append", compute)
  }
}