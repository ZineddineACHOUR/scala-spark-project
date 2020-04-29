package com.zinouapp.app.utils


import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}

object Parser {
  def getSchemaFromHeader(schemaString: String) = {

    schemaString.split(",").toList
  }

  def getSchema(headerListe: List[String]):StructType = {

   val fields: List[StructField] = headerListe.map(elt =>{
     val splited:Array[String]= elt.split(":")
     val name=splited(0)
     val dataType= splited.size match {
       case 0 => StringType
       case 1 => StringType
       case _ => {
         val dataTypeString = splited(1)
         dataTypeString.toLowerCase match {
           case "integer" => IntegerType
           case "boolean" => BooleanType
           case "double" => DoubleType
           case "string" => StringType
           case _ => StringType
         }
     }
     }
      StructField(name, dataType, true)
    })

    StructType(fields)
  }

}
