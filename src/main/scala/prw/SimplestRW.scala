package prw

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data._
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.example._
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.column.ParquetProperties

trait Consts {
  val path = new Path("hdfs://localhost:9000/data1.parquet")
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

  val groups: List[Group] = (1 to 1000000).toList.map { id =>
    groupFactory.newGroup()
      .append("id", id.toLong)
      .append("login", s"login$id")
      .append("age", (id + 10) % 50)
  }
  val blockSize = 2*1024*1024

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
