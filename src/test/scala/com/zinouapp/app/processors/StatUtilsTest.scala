package com.zinouapp.app.processors

import com.zinouapp.app.domains.{MostSoldProcuct, Sale}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, FunSuite}
import com.zinouapp.app.domains.AppImplicits._
class StatUtilsTest extends FlatSpec {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  "  recuperer les produit les plus vendus " should "Ok" in {

    //Given
    val salesDS:Dataset[Sale]=spark.createDataset(List(
      Sale(0,"2012-01-31","shop_1","kinder-cola","glass",13280,"500ml",12738.38),
      Sale(1,"2012-01-31","shop_1","kinder-cola","plastic",6727,"1.5lt",19238.04),
      Sale(2,"2012-01-31","shop_1","kinder-cola","can",13280,"500ml",12738.38),
      Sale(3,"2012-01-31","shop_1","adult-cola","glass",20050,"500ml",19994.69),
      Sale(4,"2012-01-31","shop_1","adult-cola","plastic",12229,"1.5lt",31200.35),
      Sale(5,"2012-01-31","shop_1","adult-cola","can",25696,"330ml",9998.52),
      Sale(6,"2012-01-31","shop_1","orange-power","glass",15041,"500ml",15101.42),
      Sale(7,"2012-01-31","shop_1","orange-power","plastic",15702,"1.5lt",23158.42),
      Sale(8,"2012-01-31","shop_1","orange-power","can",34578,"330ml",14715.78),
      Sale(9,"2012-01-31","shop_1","gazoza","glass",44734,"500ml",22104.5),
      Sale(10,"2012-01-31","shop_1","gazoza","plastic",26884,"1.5lt",22972.5),
      Sale(11,"2012-01-31","shop_1","gazoza","can",16367,"330ml",6580.66)))

    val expected = Array(MostSoldProcuct("gazoza",87985,51657.66))

    //When
    val result = StatProcessor.getMostSoldProductSQL(salesDS)
    println("Resultat ==> " + result.collectAsList())

    //Then
    assert(result.collect().sameElements(expected))
  }

  "Recuperer le produit le mieux vendu par magasin " should "Ok " in {

    //Given
    val salesDS:Dataset[Sale]=spark.createDataset(List(
      Sale(0,"2012-01-31","shop_1","kinder-cola","glass",13280,"500ml",12738.38),
      Sale(1,"2012-01-31","shop_1","kinder-cola","plastic",6727,"1.5lt",19238.04),
      Sale(2,"2012-01-31","shop_1","kinder-cola","can",13280,"500ml",12738.38),
      Sale(3,"2012-01-31","shop_1","adult-cola","glass",20050,"500ml",19994.69),
      Sale(4,"2012-01-31","shop_1","adult-cola","plastic",12229,"1.5lt",31200.35),
      Sale(5,"2012-01-31","shop_1","adult-cola","can",25696,"330ml",9998.52),
      Sale(6,"2012-01-31","shop_1","orange-power","glass",15041,"500ml",15101.42),
      Sale(7,"2012-01-31","shop_1","orange-power","plastic",15702,"1.5lt",23158.42),
      Sale(8,"2012-01-31","shop_1","orange-power","can",34578,"330ml",14715.78),
      Sale(9,"2012-01-31","shop_1","gazoza","glass",44734,"500ml",22104.5),
      Sale(10,"2012-01-31","shop_1","gazoza","plastic",26884,"1.5lt",22972.5),
      Sale(11,"2012-01-31","shop_1","gazoza","can",16367,"330ml",6580.66),
      Sale(15,"2012-01-31","shop_2","kinder-cola","glass",8943,"500ml",13500.39),
      Sale(16,"2012-01-31","shop_2","kinder-cola","plastic",15073,"1.5lt",24414.59),
      Sale(17,"2012-01-31","shop_2","kinder-cola","can",11174,"330ml",10202.35),
      Sale(18,"2012-01-31","shop_2","adult-cola","glass",21063,"500ml",21062.52),
      Sale(19,"2012-01-31","shop_2","adult-cola","plastic",15126,"1.5lt",39534.87),
      Sale(20,"2012-01-31","shop_2","adult-cola","can",21095,"330ml",13532.35),
      Sale(21,"2012-01-31","shop_2","orange-power","glass",21720,"500ml",19203.38),
      Sale(22,"2012-01-31","shop_2","orange-power","plastic",25347,"1.5lt",32246.88),
      Sale(23,"2012-01-31","shop_2","orange-power","can",54780,"330ml",12694.04),
      Sale(24,"2012-01-31","shop_2","gazoza","glass",41378,"500ml",24109.73),
      Sale(25,"2012-01-31","shop_2","gazoza","plastic",30757,"1.5lt",30677.44),
      Sale(26,"2012-01-31","shop_2","gazoza","can",49688,"330ml",12743.44)))

    //When

    val result = StatProcessor.getMostSoldProductByShop(salesDS)


    //Then

//    assert(res)
  }

}
