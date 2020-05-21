import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object SparkHelper {

  /* 变量 */
  //  private val nodes = "localhost"
  private val nodes = "http://172.16.0.20"

  //spark session
  val sparkSession: SparkSession = SparkSession.builder().config(getConf).getOrCreate()

  // spark context
  def getContext(): SparkContext = {
    sparkSession.sparkContext
  }

  def getConf = {
    val conf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local[*]")
    conf.set("es.nodes", nodes)
    conf.set("es.port", "9200")
    //    conf.set("es.scroll.size", "100000")
    conf
  }

  def getStructType() = {
    var list: List[StructField] = List.empty
    val id: StructField = StructField("id", DataTypes.StringType, true)
    val entity: StructField = StructField("entity", DataTypes.StringType, true)
    list = list :+ entity
    list = list :+ id
    DataTypes.createStructType(list.toArray)
  }


}
