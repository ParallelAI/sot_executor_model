package parallelai.sot.executor.model

import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat
import org.scalatest.{ MustMatchers, WordSpec }
import parallelai.sot.executor.model.SOTMacroConfig.SeqTapDefinition

class SeqTapDefinitionSpec extends WordSpec with MustMatchers {
  "SeqTapDefinition" should {
    "be instantiated" in {
      case class Test(blah: String)

      implicit val jsonFormat: RootJsonFormat[Test] = jsonFormat1(Test.apply)

      val tapDefinition = SeqTapDefinition[Test](content = Seq(Test("scooby")))

      tapDefinition.`type` mustEqual SeqTapDefinition.`type`
    }
  }
}