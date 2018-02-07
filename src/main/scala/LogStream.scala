/**
* University of Skövde
* Master in Data Science | Big data programming course
* 
* Coded by: Elio Ventocilla.
* Description: Log streaming example.
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
		ssc.checkpoint("/path/to/checkpoint/folder")


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


		// ------------------ Stateless transformation ------------------

		// 'logs' is the input DStream
		val logs = ssc.textFileStream("/path/to/logs/folder")

		// Do a request method (e.g. GET, POST, etc) count for Linux
		logs
			.filter(_.contains("Linux"))
			.map(s => (getMethod(s), 1))
			.reduceByKey(_ + _)
			.print()


		// ------------------ Stateful transformations ------------------

		// ---- Windowed

		logs
			.map(s => (getOS(s), 1))
			.reduceByKeyAndWindow((a: Int, b: Int) => {		// Only on pair DStreams.
				a + b, Seconds(20), Seconds(15)
			})
			.print()


		// ---- Update state

		/**
		* Updates the state for every key in the DStream.
		* @param newValues Corresponds to a sequence of new, incomming values
		* 	for a given key.
		* @param state Corresponds to the old state for that given key. The first time
		*	the function is called for a given key, the value of the state will be None.
		* @return The new state.
		*/
		def updateState(newValues: Seq[Int], state: Option[Int]): Option[Int] = {
			newValues.isEmpty match {
				case true => state
				case false => Some(state.getOrElse(0) + newValues.length)
			}
		}

		logs
			.map(s => (getStatus(s), 1))
			.updateStateByKey(updateState _)				// Only on pair DStreams.
			.print()


		// ----------------- Begin streaming ------------------
		
		// Start receiving data through the input DStream and run the rest of the code...
		ssc.start()
		// ...until the user requests termination.
		ssc.awaitTermination()
	}
}
