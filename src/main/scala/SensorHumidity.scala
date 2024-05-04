import java.io.File

object SensorHumidity extends App {

  var fileList: List[File] = null
  val directory = "src/main/resources/sensor/humidity"


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

  def numOfProcessedMeasurements(): Int={
    var conf = new SparkConf().setAppName("Read CSV Files From Directory").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


}
