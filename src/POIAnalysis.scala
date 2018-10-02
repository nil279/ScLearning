import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.log4j._


object POIAnalysis {
  case class Location(lat: Double, lon: Double)
  case class poloc(poi: String, loc: Double)

  // static final Logger logger = LogManager.getLogger(POIAnalysis.Object);
  
 // static Logger log = Logger.getLogger(POIAnalysis.object);
  
  def main(args: Array[String]) {
    val Requestdatalocation = "Resources\\DataSample.csv"
    val POIFileLocation = "Resources\\POIList.csv"
    val POIResult1 = "Resources\\output.csv"
      
   
     Logger.getLogger("org").setLevel(Level.OFF)
    //Logger logger = Logger.getLogger(POIAnalysis.Object);
     
     val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("POIAnalysis").set("spark.driver.host", "localhost")
    conf.set("spark.testing.memory", "2147480000")
    
    val sc = new SparkContext(conf)

    val inputFile = sc.textFile(Requestdatalocation)
    val headerrow = inputFile.first; //_ID, TimeSt,Country,Province,City,Latitude,Longitude
    val inputdata = inputFile.filter(x => x != headerrow)

    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    println("Creating RDD using input file")
    //Logger.info("Creating RDD using input file");
    
    val inputdatamap = inputdata.map(x => {
      val splitted = x.split(",")
      val ID = splitted(0).trim
      val TimeSt = splitted(1).trim
      val Country = splitted(2).trim
      val Province = splitted(3).trim
      val City = splitted(4).trim
      val Latitude = splitted(5).trim
      val Longitude = splitted(6).trim
      (ID, TimeSt, Country, Province, City, Latitude, Longitude)
    })
    val inputdataDF = inputdatamap.toDF("ID", "TimeSt", "Country", "Province", "City", "Latitude", "Longitude")
    // println(inputdataDF.printSchema())

    println("Total Rows in input file ==> " + inputdataDF.count)

    // drop fully identical rows
    //val withoutDuplicates = customerDF.dropDuplicates()
    //println("*** Now without duplicates")
    //withoutDuplicates.show()
    // drop fully identical rows
    val withoutPartials = inputdataDF.dropDuplicates(Seq("TimeSt", "Latitude", "Longitude"))
    println("*** total after removing partial duplicates ==> " + withoutPartials.count)

    /*

     */
    val Poifile = sc.textFile(POIFileLocation)
    val firstpoi = Poifile.first //POIID, Latitude,Longitude

    val Poidata = Poifile.filter(x => x != firstpoi)
    val poidatamap = Poidata.map(x => {
      val splitted = x.split(",")
      val POIID = splitted(0).trim.toString()
      val PLatitude = splitted(1).trim.toString()
      val PLongitude = splitted(2).trim.toString()
      (POIID, PLatitude, PLongitude)
    })

    //  val poidf = poidatamap.toDF("POIID", "PLatitude", "PLongitude")
    val noOfPois = poidatamap.count()
    val POIArray = poidatamap.take(noOfPois.toInt)

    /*
     *
     */

    trait DistanceCalcular {
      def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Double
    }
    class DistanceCalculatorImpl extends DistanceCalcular {
      private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
      override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Double = {
        val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
        val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
        val sinLat = Math.sin(latDistance / 2)
        val sinLng = Math.sin(lngDistance / 2)
        val a = sinLat * sinLat + (Math.cos(Math.toRadians(userLocation.lat)) * Math.cos(Math.toRadians(warehouseLocation.lat)) * sinLng * sinLng)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        (AVERAGE_RADIUS_OF_EARTH_KM * c).toDouble
      }
    }

    /*
     *
     */
    //poidatamap.cache()

    trait PoiLocations {
      def nearestpoi(poilocation: Location): Double
    }

    class NearestPOILocation extends java.io.Serializable {

      def nearestpoi(tst: Location) = {
        val matrix = Array.ofDim[String](POIArray.length, 2)
        var xx = 0
        var nearvalue = 0.00
        var retval = ""
        for (a <- POIArray) {
          val sds = new DistanceCalculatorImpl().calculateDistanceInKilometer(tst, Location(a._2.toDouble, a._3.toDouble))
          matrix(xx)(0) = a._1.toString()
          matrix(xx)(1) = sds.toString()
          if (nearvalue == 0.00) {
            nearvalue = sds
          } else {
            nearvalue = nearvalue
          }

          if (sds < nearvalue) {
            retval = a._1.toString()
            nearvalue = sds
          }
          xx = xx + 1;
        }
        val sortarray = matrix.sortBy(_(0))

        retval
      }
    }
    /*
    * "ID", "TimeSt", "Country", "Province", "City", "Latitude", "Longitude")
    */
    //val headers = sc.broadcast(poidatamap)
    val reducedinputdata = withoutPartials.map(y => {
      val ID = y.getString(0)
      val TimeSt = y.getString(1)
      val Country = y.getString(2)
      val Province = y.getString(3)
      val City = y.getString(4)
      val LAT1 = y.getString(5)
      val LONGT1 = y.getString(6)
      //val POIS = new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(51.00750, -114.00400), Location(53.546167, -113.485734))
      val POIS = new NearestPOILocation().nearestpoi(Location(LAT1.toDouble, LONGT1.toDouble))
      (ID, TimeSt, Country, Province, City, POIS)
    })
    reducedinputdata.collect().foreach(println)

    //val outputfile = reducedinputdata.saveAsTextFile("src/main/resources/output.csv")
    reducedinputdata.rdd.coalesce(1, shuffle = true).saveAsTextFile(POIResult1)

    /*
   def main(args: Array[String]) {
      println("Say Hello scala Project");
      val ss = new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(51.00750, -114.00400), Location(53.546167, -113.485734))
      println(ss);
    }
	*/
    sc.stop()
  }
}
