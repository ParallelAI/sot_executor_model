package parallelai.sot.executor.model

import org.scalatest.{Matchers, WordSpec}
import parallelai.sot.executor.model.SOTMacroConfig._
import spray.json._

class JSONDefinitionsSpec extends WordSpec with Matchers {

  "SOTMacroConfig" should {

    "parse pubsub to datastore json string" in {
      val config = SOTMacroJsonConfig("ps2ds-test.json")

      val avroSchema =
        """
          |{      "type": "record",
          |      "name": "Message",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |    }
        """.stripMargin.parseJson.asJsObject

      val inSchema = PubSubSchemaType(`type` = "PubSub", serialization = "Avro", name = "Message", schema = avroSchema, "p2pin")
      val outSchema = DatastoreSchemaType(`type` = "Datastore", schema = Some(DatastoreSchema(name = "OutSchema", fields = List(DatastoreSchemaField(name = "score2x", `type` = "Double")))))
      val dag = List(DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"))
      val steps = List(Op(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        Op(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        Op(`type` = "transformation", name = "mapper2", op = "map", func = "m => OutSchema(m._1, m._2.toString, m._2 * 0.123)"),
        Op(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""))

      val expectedConfig = Config(inSchema, outSchema, dag, steps)
      expectedConfig should be(config)

    }


  }

}
