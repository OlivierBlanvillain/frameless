package frameless

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.types.StructType

object TypedExpressionEncoder {

  /** In Spark, DataFrame has always schema of StructType
    *
    * DataFrames of primitive types become records with a single field called "_1".
    */
  def targetStructType[A](encoder: TypedEncoder[A]): StructType = {
    // if (encoder.lol) return encoder.lel.schema
    encoder.catalystRepr match {
      case x: StructType =>
        StructType(x.fields.map(_.copy(nullable = false)))
        // if (encoder.nullable)
        // else x
      // case dt => new StructType().add("_1", dt, nullable = encoder.nullable)
    }
  }

  def apply[T: TypedEncoder]: ExpressionEncoder[T] = {
    val encoder = TypedEncoder[T]


    val schema = targetStructType(encoder)

    val in = BoundReference(0, encoder.jvmRepr, true)

    val (out, toRowExpressions) = encoder.toCatalyst(in) match {
      // case If(_, _, x: CreateNamedStruct) =>
      //   val out = BoundReference(0, encoder.catalystRepr, true)

      //   (out, x.flatten)
      case other: CreateNamedStruct =>
        val out = GetColumnByOrdinal(0, encoder.catalystRepr)

        (out, other.flatten)
    }

    val ee = new ExpressionEncoder[T](
      schema = schema,
      flat = false,
      serializer = toRowExpressions,
      deserializer = encoder.fromCatalyst(out),
      clsTag = encoder.classTag
    )

    if (encoder.lol) {
      println("-----------")
      println
      println("schema:")
      println(ee.schema)
      println(encoder.lel.schema)
      println
      println("serializer:")
      println(ee.serializer.head)
      println(encoder.lel.serializer.head)
      // println(encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].targetObject.asInstanceOf[AssertNotNull].child.asInstanceOf[AssertNotNull].child)

      // println(ee.serializer.head.asInstanceOf[Alias].child)

      // println(s"targetObject   = ${encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].targetObject}")
      // println(s"functionName   = ${encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].functionName}")
      // println(s"dataType       = ${encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].dataType}")
      // println(s"arguments      = ${encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].arguments}")
      // println(s"propagateNull  = ${encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].propagateNull}")
      // println(s"returnNullable = ${encoder.lel.serializer.head.asInstanceOf[Alias].child.asInstanceOf[Invoke].returnNullable}")

      println
      println("deserializer:")
      println(ee.deserializer)
      println(encoder.lel.deserializer)
      println
      def same(a: ExpressionEncoder[T], b: ExpressionEncoder[T]): Unit = {
        val x = a.deserializer
        val y = b.deserializer
        // assert(x == y)
      }
      same(encoder.lel, ee)
      // encoder.lel
      ee
    }
    else ee
  }
}
