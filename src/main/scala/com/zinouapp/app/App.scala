package com.zinouapp.app

import com.zinouapp.app.domains.Sale
import com.zinouapp.app.utils.Parser
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.zinouapp.app.domains.AppImplicits._
/**
 * Hello world!
 *
 */

object App {

  def main(args: Array[String]): Unit = {
    println( "Hello Zinou!!!!!!, Are you ready?? \n\n" )

    implicit val spark= SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val inputFilePath= args(0)

    val header = spark.read.textFile(inputFilePath).collect()(0)
    println(header)
    val schemaString = Parser.getSchemaFromHeader(header)
    val schema: StructType = Parser.getSchema(schemaString)
    val df= spark.read.schema(schema).option("delimiter", ",").option("header","true").csv(inputFilePath)
        df.printSchema()
        df.show()

    println(" Number of rows ==>  " + df.collect().size + "\n\n")

    df.map(elt => {
      Sale(elt.getInt(0),elt.getString(1),elt.getString(2),elt.getString(3),elt.getString(4),elt.getInt(5),elt.getString(6),elt.getDouble(7))
    }).write.partitionBy("shop", "date").parquet("data/salesoutput.parquet")

    println("Good bye Zinou!!!!!!!, You are the best\n\n")


  }
}
