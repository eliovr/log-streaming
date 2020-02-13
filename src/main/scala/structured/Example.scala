/**
* University of Skövde
* Master in Data Science | Big data programming course
*
* Coded by: Elio Ventocilla.
* Description: Log streaming example using Structured Streaming.
*/

package structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
* This case class will be use to give the data a structure.
* It needs to be defined outside, or the compiler will complain.
*/
case class Schema(os: String, method: String, status: Int)

object Example {
	def main(arg: Array[String]) = {

		// -------------------- Configuration ---------------------------

		val spark = SparkSession.builder.appName("Log streaming").getOrCreate()
		import spark.implicits._

		// Set log level to warn so a to silence the output
		spark.sparkContext.setLogLevel("WARN")

		// ------------------ Parsing methods ----------------------------

		// Get OS from the given string
		def getOS(s: String): String =
			if (s.contains("Windows"))
				"Windows"
			else if (s.contains("Mac"))
				"Mac"
			else
				"Linux"

		def getMethod(s: String): String =
			s.dropWhile(_ != '"')
			.tail
			.takeWhile(_ != ' ')

		def getStatus(s: String): Int = {
			val i = s.indexOf("\" ")
			s.substring(i + 2, i + 5).toInt
		}


		// ------------------ INSTANTIATE DATASET ------------------

		val logs = spark
			.readStream
			.textFile("/tmp/log-files/")


		// ------------------ TRANSFORMATIONS ------------------

		// Create a schema for the data using the Schema case class.
		// Then filter out linux requests and perform a count (aggregation) by
		// os, method, ans status.
		val counts = logs
			.map(s => Schema(getOS(s), getMethod(s), getStatus(s)))
			.filter(_.os == "Linux")
			.groupBy("os", "method", "status")
			.count


		// ----------------- BEGIN STREAMING ------------------

		// The action is defined here, i.e., what you do with the data.
		val query = counts
			.writeStream
			.outputMode("complete")
			.format("console")
			.start()

		query.awaitTermination()
	}
}
