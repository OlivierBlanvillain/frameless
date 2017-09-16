package frameless

import scala.annotation.implicitNotFound

/**
  * When averaging Spark doesn't change these types:
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  * But it changes these types :
  * - Int        -> Double
  * - Short      -> Double
  * - Long       -> Double
  */
@implicitNotFound("Cannot compute average of type ${In}.")
trait CatalystAverageable[In, Out]

object CatalystAverageable {
  implicit val averageableBigDecimal: CatalystAverageable[BigDecimal, BigDecimal] = new CatalystAverageable[BigDecimal, BigDecimal] {}
  implicit val averageableDouble: CatalystAverageable[Double, Double] = new CatalystAverageable[Double, Double] {}
  implicit val averageableLong: CatalystAverageable[Long, Double] = new CatalystAverageable[Long, Double] {}
  implicit val averageableInt: CatalystAverageable[Int, Double] = new CatalystAverageable[Int, Double] {}
  implicit val averageableShort: CatalystAverageable[Short, Double] = new CatalystAverageable[Short, Double] {}
}
