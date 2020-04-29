package com.zinouapp.app.domains

import org.apache.spark.sql.Encoders

object AppImplicits {

  implicit val encodeSale = Encoders.product[Sale]
  implicit val encodeMostSoldProduct = Encoders.product[MostSoldProcuct]




}
