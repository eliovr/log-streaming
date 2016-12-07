/**
* University of Skövde | Big data programming
* Log streaming example.
* 
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._

import org.apache.spark.sql.functions._

object LogStream {
	def main(arg: Array[String]) = {

		// -------------------- Configuration ---------------------------

		val conf = new SparkConf().setAppName("Reddit comments")

		// Read every 5 seconds
		val ssc = new StreamingContext(conf, Seconds(5))

		// Set log level to warn so a to silence the output
		ssc.sparkContext.setLogLevel("WARN")

		// Checkpoint is needed for stateful transformations
		// Spark should have write permissions on given folder
		ssc.checkpoint("<path to a checkpoint folder>")

		// ---------------------------------------------------------------



		// ------------------ Parsing methods ----------------------------

		// Get OS from the given string
		def getOS(s: String): String = 
			if (s.contains("Windows")) 
				"Windows"
			else if (s.contains("Mac"))
				"Mac"
			else
				"Linux"

		def getRequestMethod(s: String): String =
			s.dropWhile(_ != '"').tail.takeWhile(_ != ' ')

		def getStatus(s: String): Int = {
			val i = s.indexOf("\" ")
			s.substring(i + 2, i + 5).toInt
		}

		// --------------------------------------------------------------




		// ------------------ Stateless transformation ------------------

		// 'logs' is the input DStream
		val logs = ssc.textFileStream("<path to logs' folder>")

		// Do a request method (e.g. GET, POST, etc) count for Linux
		val linuxLogs = logs.filter(_.contains("Linux"))
		val requestPairs = linuxLogs.map(s => (getRequestMethod(s), 1))
		val requestCount = requestPairs.reduceByKey(_ + _)

		// Every 5 seconds...
		requestCount.print()

		// ---------------------------------------------------------------




		// ------------------ Statefull transformations ------------------

		// Window operation
		val osPairs = logs.map(s => (getOS(s), 1))
		val osCounts = osPairs.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(15))
		osCounts.print()


		// Update state on window
		def updateState(newValues: Seq[Int], state: Option[Int]): Option[Int] = {
			var n = 0
			if (newValues.size > 0)
				n = newValues.head

			Some(state.getOrElse(0) + n)
		}

		val osState = osCounts.updateStateByKey(updateState _)
		osState.print()

		// ---------------------------------------------------------------



		// Start receiving data through the input DStream and run the rest of the code...
		ssc.start()
		// ...until the user requests termination.
		ssc.awaitTermination()
	}
}
