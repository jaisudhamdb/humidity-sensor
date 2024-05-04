import java.io.File
import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object SensorHumidity extends App {

  var fileList: List[File] = null
  val directory = "src/main/resources/sensor/humidity"
  var humidityListBuffer: ListBuffer[String] = scala.collection.mutable.ListBuffer[String]
  var sensorIdListBuffer: ListBuffer[String] = scala.collection.mutable.ListBuffer[String]
  var finalMap: Map[String, ListBuffer[Int]] = Map()


  val filesProcessed = noOfProcessedFiles(directory)
  println(s"Number of files processed = $filesProcessed")

  transformMeasurements()

  val invalidHumidityValuesCount = numOfFailedMeasurements(humidityListBuffer)
  println(s"Number of invalid humidity values present in the processed files = $invalidHumidityValuesCount")
  println(s"Number of processed measuredments = (${humidityListBuffer.length} - $invalidHumidityValuesCount)")

  findMinMaxAvgValues(humidityListBuffer, sensorIdListBuffer)

  def noOfProcessedFiles(d: String) = {
    val dir = new File(d)
    if (dir.exists && dir.isDirectory) {
      fileList = dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
    fileList.size
  }

  def transformMeasurements() = {
    var conf = new SparkConf().setAppName("Read CSV Files From Directory").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    for (file <- fileList) {
      val textRDD = sc.textFile(file.toString)
      val header = textRDD.first()
      val textRDDResult = textRDD.filter(row => row != header)
      val empRdd = textRDDResult.map {
        line =>
          val col = line.split(",")
          var humidityList = col(0)
          var sensorIdList = col(1)
          humidityListBuffer += humidityList
          sensorIdListBuffer += sensorIdList
      }
    }
  }

  def numOfFailedMeasurements(humidityListBuffer: ListBuffer[String]): Int = {
    val invalidHumidityValues = humidityListBuffer.toList
    var invalidHumidityCount = 0
    for (i <- 0 until invalidHumidityValues.length) {
      if (invalidHumidityValues(i).equals("NaN")) {
        invalidHumidityCount = invalidHumidityCount + 1
        humidityListBuffer(i) = "0" // replacing the NaN values with 0, so that it's easy to operate on humidityList to find average.
      }
    }
    invalidHumidityCount
  }


  def findMinMaxAvgValues(humidityListBuffer: ListBuffer[String], sensorIdListBuffer: ListBuffer[String]) = {

    for (i <- sensorIdListBuffer) {
      if (!finalMap.contains(i)) {
        finalMap.updated(i, ListBuffer(0))
      }
      else {
        var value = finalMap.get(i)
        finalMap.updated(i, value += humidityListBuffer(i.toInt)) // finalMap(s1 -> (100,97,21), s2 -> (10,50,120) ...)
      }
    }

    var resultDf: Map[Int, ListBuffer[Int]] = Map()
    val min = for (x <- finalMap) {
      var s = x._1
      var min = x._2.min
      var max = x._2.max
      var avg = x._2.sum / x._2.size
      resultDf.updated(avg, ListBuffer(s, min, max))
      printInOrder(resultDf)
    }
  }

  def printInOrder(resultDf: Map[Int, ListBuffer[Int]]) = {
    val r: (immutable.Iterable[Int], immutable.Iterable[ListBuffer[Int]]) = resultDf.unzip
    val sortedAverageHumidity = r._1.toArray.sorted
    val result = sortedAverageHumidity.zip(r._2).toMap
    result.map { x =>
      var value: Option[ListBuffer[Int]] = result.get(x._1)
      println(s"sensor id = ${value.get(0)}, with a minimum of ${value.get(1)}, maximum of ${value.get(2)} and average of ${x._1}")
    }
  }


}
