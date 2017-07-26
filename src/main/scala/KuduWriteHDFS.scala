import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Kasim on 2017/7/26.
  */
object KuduWriteHDFS {
  case class TableStructureVehiclePosition(vehicleno : String, platecolor : Int,
                                           positiontime : Long, accesscode : Int,
                                           city : Int, curaccesscode : Int,
                                           trans : Int, updatetime : Long,
                                           encrypt : Int, lon : Int, lat : Int,
                                           vec1 : Int, vec2 : Int, vec3 : Int,
                                           direction : Int, altitude : Int,
                                           state : Long, alarm : Long,
                                           reserved : String, errorcode : String,
                                           roadcode : Int)

  def main(args: Array[String]): Unit = {
    // need to input table name
    if(args.length < 1) System.exit(1)

    val conf = new SparkConf().setAppName("KuduWriteHDFS").setMaster("yarn")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    // to get kudu data
    val kuduContext = sparkSession.sparkContext.broadcast(new KuduContext("nn01"))

    import sparkSession.implicits._
    kuduContext.value.kuduRDD(sparkSession.sparkContext, "impala::position.CTTIC_VehiclePosition_" + args(0)).
      map(record=> {
        val recordArray = record.toString().split(",")
        TableStructureVehiclePosition(recordArray(0), recordArray(1).toInt, recordArray(2).toLong,
          recordArray(3).toInt, recordArray(4).toInt, recordArray(5).toInt, recordArray(6).toInt,
          recordArray(7).toLong, recordArray(8).toInt, recordArray(9).toInt, recordArray(10).toInt,
          recordArray(11).toInt, recordArray(12).toInt, recordArray(13).toInt, recordArray(14).toInt,
          recordArray(15).toInt, recordArray(16).toLong, recordArray(17).toLong, recordArray(18),
          recordArray(19), recordArray(20).toInt)
      }).toDF().write.mode("Append").parquet("hdfs://nameservice1/VP/VehiclePosition_" + args(0))
  }

}
