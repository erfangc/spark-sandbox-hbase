import org.apache.hadoop.hbase.{TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by erfangc on 6/7/15.
 */
object HBaseLoader extends App {
  val logger = LoggerFactory.getLogger(getClass)
  val lines: Iterator[String] = Source.fromFile("/home/erfangc/accounts.csv").getLines()
  // read the input CSV file from local FS
  val headers = lines.take(1).next().split(",")
  val entries = lines.map(line => {
    headers.zip(line.split(",")).toMap
  }).toSeq

  // create put operations for the entires
  val puts = generatePuts(entries)

  // create an hbase connection
  // Instantiating configuration class
  val conf = HBaseConfiguration.create();

  // Instantiating HbaseAdmin class
  val mgr = ConnectionFactory.createConnection(conf)
  val table = mgr.getTable(TableName.valueOf("accounts"))

  // send the puts to HBase
  table.put(puts)
  logger.info(s"${puts.size} put ops complete, go check hbase now ....")
  table.close()

  def generatePuts(entries: Seq[Map[String, String]]): List[Put] = {
    entries.map(entry => {
      val id = entry.get("id").get
      val cf: Array[Byte] = Bytes.toBytes("default")

      val firstName = entry.get("first_name").get
      val lastName = entry.get("last_name").get
      val email = entry.get("email").get
      val country = entry.get("country").get
      val balance = entry.get("balance").get

      val put = new Put(Bytes.toBytes(id))
      put.addColumn(cf, Bytes.toBytes("first_name"), Bytes.toBytes(firstName))
      put.addColumn(cf, Bytes.toBytes("last_name"), Bytes.toBytes(lastName))
      put.addColumn(cf, Bytes.toBytes("email"), Bytes.toBytes(email))
      put.addColumn(cf, Bytes.toBytes("country"), Bytes.toBytes(country))
      put.addColumn(cf, Bytes.toBytes("balance"), Bytes.toBytes(balance))
      put

    }).toList
  }

}
