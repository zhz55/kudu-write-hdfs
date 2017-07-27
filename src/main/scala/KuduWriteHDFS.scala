import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Kasim on 2017/7/26.
  */
object KuduWriteHDFS {

  def main(args: Array[String]): Unit = {
    // need to input table name
    if(args.length < 1) System.exit(1)

    val conf = new SparkConf().setAppName("KuduWriteHDFS").setMaster("yarn")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    // to get kudu data
    val kuduContext = sparkSession.sparkContext.broadcast(new KuduContext("nn01"))

    import sparkSession.implicits._
    kuduContext.value.kuduRDD(sparkSession.sparkContext, "impala::position.CTTIC_VehiclePosition_" + args(0),
      Seq("vehicleno", "platecolor", "positiontime", "accesscode", "city", "curaccesscode",
        "trans", "updatetime", "encrypt", "lon", "lat", "vec1", "vec2", "vec3", "direction",
        "altitude", "state", "alarm", "errorcode", "roadcode")).
      map{
        case Row(vehicleno : String, platecolor : Int,
                positiontime : Long, accesscode : Int,
                city : Int, curaccesscode : Int,
                trans : Int, updatetime : Long,
                encrypt : Int, lon : Int, lat : Int,
                vec1 : Int, vec2 : Int, vec3 : Int,
                direction : Int, altitude : Int,
                state : Long, alarm : Long,
                errorcode : String, roadcode : Int)
        => (vehicleno, platecolor, positiontime, accesscode, city, curaccesscode,
          trans, updatetime, encrypt, lon, lat, vec1, vec2, vec3, direction,
          altitude, state, alarm, errorcode, roadcode)
      }.toDF().write.mode("Append").parquet("hdfs://nameservice1/VP/VehiclePosition_" + args(0))
  }
}
