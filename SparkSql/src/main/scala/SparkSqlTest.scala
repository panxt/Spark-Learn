import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._ //2

object SparkSqlTest {

  private val context: SparkContext = SparkHelper.getContext()
  private val sc: SparkSession = SparkHelper.sparkSession

  import sc.implicits._

  private val sqlContext: SQLContext = sc.sqlContext

  def main(args: Array[String]): Unit = {
    val flow14 = sqlContext
      .read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.field.include" -> "device"))
      .load("data_flow-2020-04-14/doc")
//    println(flow14.count())
    flow14.createOrReplaceTempView("flow14")
//    val frame: DataFrame = sqlContext.sql("select uploadloglevelType,count(*) from flow14 where uploadloglevelType like '网络%' group by uploadloglevelType ")
    val frame: DataFrame = sqlContext.sql("select device,count(*) from flow14 group by device having device like '10.163.1.1%' ")

    frame.show()

//    sc.sql("CREATE TEMPORARY VIEW data_flow USING org.elasticsearch.spark.sql OPTIONS (path 'data_flow-2020-04-14/doc',es.read.field.include 'device_id' )");
//    val frame: DataFrame = sc.sql("select count(*) from data_flow")
//    println(frame.count())


  }

}
