package com.zinouapp.app.utils


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FlatSpec

import scala.collection.mutable

class ParserTest extends FlatSpec  {

  "Parser le header d'une data table en schema " should "OK" in{

    //Given
    val headerListe:List[String] = List("asin","name","rating","date","verified","title","body","helpfulVotes")
    val expectedSchema: StructType = StructType(List(
      StructField("asin", StringType,true),
      StructField("name", StringType,true),
      StructField("rating", StringType,true),
      StructField("date", StringType,true),
      StructField("verified", StringType,true),
      StructField("title", StringType,true),
      StructField("body", StringType,true),
      StructField("helpfulVotes", StringType,true)
    ))

    //When

    val resultSchema = Parser.getSchema(headerListe)

    println("Result ==> " + resultSchema)

    //Then
    assert(resultSchema==expectedSchema)
  }

  "Parser un fichier CSV pour recuperer le schema " should "ok" in {

    //Given
    val schemaString = "asin,name,rating"

    val expected = List("asin","name","rating")
//    val expected : StructType = StructType(List(
//      StructField("asin", StringType,true),
//      StructField("name", StringType,true),
//      StructField("rating", StringType,true)
//      ))

    //When

    val resultSchema = Parser.getSchemaFromHeader(schemaString)
    println("Result ==> " +  resultSchema)

    println("Expected ==> " +  expected)
    //Then

    assert(resultSchema.sameElements(expected))


  }


}
