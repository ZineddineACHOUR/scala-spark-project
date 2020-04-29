package com.zinouapp.app.processors

import com.zinouapp.app.domains.{MostSoldProcuct, Sale}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, first, max}
import com.zinouapp.app.domains.AppImplicits._
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._


object StatProcessor {
  def getMostSoldProductByShop(salesDS: Dataset[Sale])(implicit spark:SparkSession) = {

    salesDS
      .groupBy("shop","brand")
      .agg(sum("quantity").as("quantities"))

//      .agg(//first("shop"),
//        first("brand"))
      //.groupBy("shop","brand")
       // max("quantities"))
    //.show()

  }


  def getMostSoldProductSQL(salesDS: Dataset[Sale])(implicit spark:SparkSession) = {

    salesDS.createOrReplaceTempView("salesTable")
    spark.sql(
      """        |
        |select brand, Sum(quantity) As totalQuantity,Sum(sales) AS totalSales
        |from salesTable
        |group by brand
        |order by totalQuantity DESC
        |limit 1
        |""".stripMargin).as[MostSoldProcuct]

  }

}
