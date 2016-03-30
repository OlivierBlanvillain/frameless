package frameless

import shapeless._
import shapeless.ops.hlist.{Length, Fill, Tupler}
import shapeless.ops.traversable.FromTraversable
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.Row

trait TypeableRow[S <: Product] extends Serializable {
  def apply(row: Row): S
  type Repr <: HList
}

trait LowPriorityTypeableRow {
  type Aux[S <: Product, R0 <: HList] = TypeableRow[S] { type Repr = R0 }

  implicit def typeableRowProduct[S <: Product, G <: HList]
    (implicit
      g: Generic.Aux[S, G],
      c: TypeTag[G],
      n: FromTraversable[G]
    ): Aux[S, G] =
      new TypeableRow[S] {
        type Repr = G
        def apply(row: Row): S = n(row.toSeq).fold(fail(row))(g.from)
      }

  protected def fail[G](row: Row)(implicit c: TypeTag[G]) =
    throw new RuntimeException(s"Type error: failed to cast row $row of type ${row.schema} to $c")
}

object TypeableRow extends LowPriorityTypeableRow {
  def apply[S <: Product](implicit t: TypeableRow[S]): Aux[S, t.Repr] = t

  implicit def typeableRowTuple[S <: Product, G <: HList, N <: Nat, F <: HList, T <: Product]
    (implicit
      t: IsTuple[S],
      g: Generic.Aux[S, G],
      c: TypeTag[G],
      l: Length.Aux[G, N],
      f: Fill.Aux[N, Any, F],
      n: FromTraversable[F],
      p: Tupler.Aux[F, T]
    ): Aux[S, G] =
      new TypeableRow[S] {
        type Repr = G
        def apply(row: Row): S = n(row.toSeq).fold(fail(row))(l => p(l).asInstanceOf[S])
      }
}
