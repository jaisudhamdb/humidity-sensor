import java.io.File

object SensorHumidity extends App {

  var fileList: List[File] = null
  val directory = "src/main/resources/sensor/humidity"
  var humidityListBuffer = scala.collection.mutable.ListBuffer[String]
  var sensorIdListBuffer = scala.collection.mutable.ListBuffer[String]

  val filesProcessed = noOfProcessedFiles(directory)
  println(s"Number of files processed = $filesProcessed" )

  def noOfProcessedFiles(d :String) = {
    val dir = new File(d)
    if (dir.exists && dir.isDirectory) {
      fileList = dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
     fileList.size
  }

  def numOfProcessedMeasurements(): Int= {
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
          (humidityList, sensorIdList)
      }
    }
  }

  def numOfFailedMeasurements():Int={
    val HumidityListwithNaNData : List[String]=humidityListBuffer.toList.flatten
    var count=0
    for (i<- 0 until (humidityListBuffer.toList.flatten.length))
    {
      if(HumidityListwithNaNData(i).equals("NaN") )
      {
        count=count+1
      }
    }
    count
  }



}
