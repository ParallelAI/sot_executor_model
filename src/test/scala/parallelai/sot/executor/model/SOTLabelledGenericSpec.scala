package parallelai.sot.executor.model

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import shapeless._
import spray.json.JsArray
import org.scalatest.{ MustMatchers, WordSpec }
import parallelai.sot.executor.model.SOTMacroConfig.{ AvroDefinition, AvroSchema, Schema }

class SOTLabelledGenericSpec extends WordSpec with MustMatchers {
  "Labelled Generic" should {
    "work on method in class" in {
      class Blah[T: TypeTag: ClassTag] {
        def blah[A <: T, L <: HList](t: A)(implicit gen: LabelledGeneric.Aux[A, L]) = gen.to(t)
      }

      val blah = new Blah[Schema]

      val v = blah.blah(AvroSchema("avro", "id", "name", "version", AvroDefinition("record", "name", "namespace", JsArray())))
      println(v)
    }

    "work on method" in {
      def blah[A, L <: HList](t: A)(implicit gen: LabelledGeneric.Aux[A, L]) = gen.to(t)

      val v = blah(AvroSchema("avro", "id", "name", "version", AvroDefinition("record", "name", "namespace", JsArray())))
      println(v)
    }
  }
}