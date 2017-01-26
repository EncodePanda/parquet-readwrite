package prw

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.parquet.example.data._
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.example._
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.column.ParquetProperties
import org.apache.hadoop.mapred.Reporter

import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType


trait Consts {
  val path = new Path("hdfs://localhost:9000/data3.parquet")
  val noOfRows = 100000
  val blockSize = 1048576
}

object SimplestWrite extends App with Consts {

  val schema = MessageTypeParser.parseMessageType(
    "message User {\n" +
      "   required int64 id;\n" +
      "   required binary login (UTF8);\n" +
      "   required int32 age;\n" +
    "}"
  )

  val groupFactory = new SimpleGroupFactory(schema)

  val groups: List[Group] = (1 to noOfRows).toList.map { id =>
    groupFactory.newGroup()
      .append("id", id.toLong)
      .append("login", s"login$id")
      .append("age", (id + 10) % 50)
  }


  val conf = new Configuration()
  conf.set( "dfs.block.size", blockSize.toString)

  val writeSupport = new GroupWriteSupport()
  GroupWriteSupport.setSchema(schema, conf)
  val writer = new ParquetWriter[Group](path, writeSupport,
    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
    blockSize,
    ParquetWriter.DEFAULT_PAGE_SIZE,
    ParquetWriter.DEFAULT_PAGE_SIZE, 
    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
    ParquetProperties.WriterVersion.PARQUET_1_0, conf)

  groups.foreach { group =>
    writer.write(group)
  }
  writer.close()
}

object SimplestRead extends App with Consts {
  val readSupport = new GroupReadSupport()
  val reader = new ParquetReader[Group](path, readSupport)
  val result: Group = reader.read()
  println(result)
}

class AltGroupReadSupport(projection: MessageType) extends GroupReadSupport {
  override def init(
    configuration: Configuration,
    keyValueMetaData: java.util.Map[String, String],
    fileSchema: MessageType): ReadContext = {
    new ReadContext(projection)
  }
}

object SimplestReadWithProjection extends App with Consts {

  val conf: Configuration = new Configuration()
  val projection = MessageTypeParser.parseMessageType(
    "message User {\n" +
      "   required binary login (UTF8);\n" +
      "}"
  )

  val readSupport = new AltGroupReadSupport(projection)
  val reader = new ParquetReader[Group](path, readSupport)
  val result: Group = reader.read()
  println(result)
}


object BlockRead extends App with Consts {
  val conf: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val fileStatus = fs.getFileStatus(path)
  val blocks = fs.getFileBlockLocations(fileStatus,0,fileStatus.getLen())
  val splits = blocks.map (b => {
    new FileSplit(path,b.getOffset(),b.getLength(),b.getHosts())
  })

  val readSupport = new GroupReadSupport()

  println(s"Number of splits: ${splits.length}")

  splits.foreach { split => {
    println(s"Next split")
    val prr = new ParquetRecordReader(readSupport)
    prr.initialize(split, conf, Reporter.NULL)
    while(prr.nextKeyValue()) {
      println(prr.getCurrentValue().getString("login", 0))
    }
  }}
}

object BlockReadWithProjection extends App with Consts {
  val conf: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val fileStatus = fs.getFileStatus(path)
  val blocks = fs.getFileBlockLocations(fileStatus,0,fileStatus.getLen())
  val splits = blocks.map (b => {
    new FileSplit(path,b.getOffset(),b.getLength(),b.getHosts())
  })

  val projection: MessageType = MessageTypeParser.parseMessageType(
    "message User {\n" +
      "   required age;\n" +
      "}"
  )

  val readSupport = new AltGroupReadSupport(projection)

  println(s"Number of splits: ${splits.length}")

  splits.foreach { split => {
    println(s"Next split")
    val prr = new ParquetRecordReader(readSupport)
    prr.initialize(split, conf, Reporter.NULL)
    while(prr.nextKeyValue()) {
      println(prr.getCurrentValue())
    }
  }}
}




