package prw

import org.apache.parquet.example.data._
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.example._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

object SimplestRead extends App {

  val path = new Path("/Users/rabbit/example.parquet")
  val readSupport = new GroupReadSupport()
  val reader = new ParquetReader[Group](path, readSupport)
  val result: Group = reader.read()
  
}
