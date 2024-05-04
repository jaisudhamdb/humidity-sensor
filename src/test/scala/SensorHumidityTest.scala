package test.scala

import java.io.File


class SensorHumidityTest extends FlatSpec with Matchers {

  it should "validate correct number of processed files given a directory" in {
    val directory = "src/test/resources/humidity"
    var fileList: List[File] = null

    val d = new File(directory)
    if (d.exists && d.isDirectory) {
      fileList = d.listFiles.filter(_.isFile).toList
      //println(fileList)
    } else {
      List[File]()
    }
    val noOfFiles = SensorHumidity.numOfProcessedFiles(directory)
    assert(noOfFiles === fileList.length)
  }

  it should "evaludate number of failed measurements" in {
    val l = List("1", "2", "3", "4", "5", "NaN")
    val r = SensorHumidity.numOfFailedMeasurements()
    assert(r.equals(1))
  }

}
