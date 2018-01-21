package frameless

import frameless.syntax._
import frameless.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column, FramelessInternals}
import shapeless.ops.record.Selector
import shapeless._
import scala.annotation.implicitNotFound

sealed trait UntypedExpression[T] {
  def expr: Expression
  def uencoder: TypedEncoder[_]
  override def toString: String = expr.toString()
}

/** Expression used in `select`-like constructions.
  *
  * Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * @tparam T phantom type representing the dataset on which this columns is
  *           selected. When `T = A with B` the selection is on either A or B.
  * @tparam U type of column
  */
sealed class TypedColumn[T, U](
  val expr: Expression)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] { self =>
  /** From an untyped Column to a [[TypedColumn]]
    *
    * @param column a spark.sql Column
    * @param uencoder encoder of the resulting type U
    */
  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  /** Fall back to an untyped Column
    */
  def untyped: Column = new Column(expr)

  private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  private def equalsTo[TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] = withExpr {
    if (uencoder.nullable && uencoder.catalystRepr.typeName != "struct") EqualNullSafe(self.expr, other.expr)
    else EqualTo(self.expr, other.expr)
  }.typed

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === 1 )
    * }}}
    *
    * apache/spark
    */
  def ===(other: U): TypedColumn[T, Boolean] = equalsTo[T, T](lit(other))

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def ===[TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] = equalsTo[TT, W](other)

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def =!=[TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] = withExpr {
    Not(equalsTo(other).expr)
  }.typed

  /** Inequality test.
    * {{{
    *   df.filter( df.col('a) =!= "a" )
    * }}}
    *
    * apache/spark
    */
  def =!=(other: U): TypedColumn[T, Boolean] = withExpr {
    Not(equalsTo(lit(other)).expr)
  }.typed

  /** True if the current expression is an Option and it's None.
    *
    * apache/spark
    */
  def isNone(implicit isOption: U <:< Option[_]): TypedColumn[T, Boolean] =
    equalsTo[T, T](lit[U,T](None.asInstanceOf[U]))

  /** True if the current expression is an Option and it's not None.
    *
    * apache/spark
    */
  def isNotNone(implicit isOption: U <:< Option[_]): TypedColumn[T, Boolean] = withExpr {
    Not(equalsTo(lit(None.asInstanceOf[U])).expr)
  }.typed

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) plus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def plus[TT, W](other: TypedColumn[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): TypedColumn[W, U] =
    self.untyped.plus(other.untyped).typed

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) + people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def +[TT, W](other: TypedColumn[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W] { type Out = W }): TypedColumn[W, U] = plus[TT, W](other)

  /** Sum of this expression (column) with a constant.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people('height) + 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def +(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = self.untyped.plus(u).typed

  /** Unary minus, i.e. negate the expression.
    * {{{
    *   // Select the amount column and negates all values.
    *   df.select( -df('amount) )
    * }}}
    *
    * apache/spark
    */
  def unary_-(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = (-self.untyped).typed

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) minus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def minus[TT, W](other: TypedColumn[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): TypedColumn[W, U] =
    self.untyped.minus(other.untyped).typed

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) - people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def -[TT, W](other: TypedColumn[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): TypedColumn[W, U] = minus[TT, W](other)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people('height) - 1 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def -(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = self.untyped.minus(u).typed

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) multiply people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def multiply[TT, W](other: TypedColumn[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): TypedColumn[W, U] =
    self.untyped.multiply(other.untyped).typed

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *[TT, W](other: TypedColumn[TT, U])(implicit n: CatalystNumeric[U], w: With.Aux[T, TT, W]): TypedColumn[W, U] = multiply[TT, W](other)

  /** Multiplication of this expression a constant.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = self.untyped.multiply(u).typed

  /** Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param u another column of the same type
    *
    * apache/spark
    */
  def divide[TT, Out: TypedEncoder, W](other: TypedColumn[TT, U])(implicit n: CatalystDivisible[U, Out], w: With.Aux[T, TT, W]): TypedColumn[W, Out] =
    self.untyped.divide(other.untyped).typed

  /** Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param other another column of the same type
    *
    * apache/spark
    */
  def /[TT, Out, W]
    (other: TypedColumn[TT, U])
    (implicit
      n: CatalystDivisible[U, Out],
      e: TypedEncoder[Out],
      w: With.Aux[T, TT, W]
    ): TypedColumn[W, Out] =
      divide[TT, Out, W](other)

  /** Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / 2 )
    * }}}
    *
    * @param u a constant of the same type
    *
    * apache/spark
    */
  def /(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[T, Double] = self.untyped.divide(u).typed

  /** Casts the column to a different type.
    * {{{
    *   df.select(df('a).cast[Int])
    * }}}
    */
  def cast[A: TypedEncoder](implicit c: CatalystCast[U, A]): TypedColumn[T, A] =
    self.untyped.cast(TypedEncoder[A].catalystRepr).typed
}

/** Expression used in `groupBy`-like constructions.
  *
  * @tparam T type of dataset
  * @tparam U type of column for `groupBy`
  */
sealed class TypedAggregate[T, U](val expr: Expression)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] {

  def this(column: Column)(implicit uenc: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }
}

object TypedColumn {
  /** Evidence that type `T` has column `K` with type `V`.
    */
  @implicitNotFound(msg = "No column ${K} of type ${V} in ${T}")
  trait Exists[T, K, V]

  @implicitNotFound(msg = "No columns ${K} of type ${V} in ${T}")
  trait ExistsMany[T, K <: HList, V]

  object ExistsMany {
    implicit def deriveCons[T, KH, KT <: HList, V0, V1](
      implicit
      head: Exists[T, KH, V0],
      tail: ExistsMany[V0, KT, V1]
    ): ExistsMany[T, KH :: KT, V1] = new ExistsMany[T, KH :: KT, V1] {}

    implicit def deriveHNil[T, K, V](
      implicit
      head: Exists[T, K, V]
    ): ExistsMany[T, K :: HNil, V] = new ExistsMany[T, K :: HNil, V] {}
  }

  object Exists {
    def apply[T, V](column: Witness)(
      implicit
      exists: Exists[T, column.T, V]
    ): Exists[T, column.T, V] = exists

    implicit def deriveRecord[T, H <: HList, K, V](
      implicit
      lgen: LabelledGeneric.Aux[T, H],
      selector: Selector.Aux[H, K, V]
    ): Exists[T, K, V] = new Exists[T, K, V] {}
  }

  implicit class OrderedTypedColumnSyntax[T, U: CatalystOrdered](col: TypedColumn[T, U]) {
    def <  [TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] =
      (col.untyped <  other.untyped).typed

    def <= [TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] =
      (col.untyped <= other.untyped).typed

    def >  [TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] =
      (col.untyped >  other.untyped).typed

    def >= [TT, W](other: TypedColumn[TT, U])(implicit w: With.Aux[T, TT, W]): TypedColumn[W, Boolean] =
      (col.untyped >= other.untyped).typed


    def <  (other: U): TypedColumn[T, Boolean] = (col.untyped <  lit(other)(col.uencoder).untyped).typed
    def <= (other: U): TypedColumn[T, Boolean] = (col.untyped <= lit(other)(col.uencoder).untyped).typed
    def >  (other: U): TypedColumn[T, Boolean] = (col.untyped >  lit(other)(col.uencoder).untyped).typed
    def >= (other: U): TypedColumn[T, Boolean] = (col.untyped >= lit(other)(col.uencoder).untyped).typed
  }
}

/** Compute the intersection of two types:
  *
  * - With[A, A] = A
  * - With[A, B] = A with B (when A != B)
  *
  * This type function is needed to prevent IDEs from infering large types
  * with shape `A with A with ... with A`. These types could be confusing for
  * both end users and IDE's type checkers.
  */
trait With[A, B] { type Out }

trait LowPrioWith {
  type Aux[A, B, W] = With[A, B] { type Out = W }
  protected[this] val theInstance = new With[Any, Any] {}
  protected[this] def of[A, B, W]: With[A, B] { type Out = W } = theInstance.asInstanceOf[Aux[A, B, W]]
  implicit def identity[T]: Aux[T, T, T] = of[T, T, T]
}

object With extends LowPrioWith {
  implicit def combine[A, B]: Aux[A, B, A with B] = of[A, B, A with B]
}
