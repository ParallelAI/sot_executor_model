package parallelai.sot.executor.model

import org.scalatest.{FlatSpec, MustMatchers}
import parallelai.sot.executor.model.SOTMacroConfig._
import parallelai.sot.executor.model.SOTMacroJsonConfig._
import spray.json._

class KafkaTapDefinitionSpec extends FlatSpec with MustMatchers {
  "KafkaTapDefinition" should "be of expected type" in {

      val tapDefinition = KafkaTapDefinition(id = "my-id", bootstrap = "my-bootstrap", topic = "my-topic", group = None, defaultOffset = None)

      tapDefinition.`type` mustEqual KafkaTapDefinitionType.`type`
  }

  it should  "be correctly initialised as source and sink" in {

    val config = SOTMacroJsonConfig("kf2kf-json-test.json")

    val inSchema =
      """
        |{
        |      "type": "json",
        |      "id": "jsonschema1",
        |      "name": "jsonschema1Name",
        |      "version": "version1",
        |      "definition": {
        |        "type": "jsondefinition",
        |        "name": "Test",
        |        "fields": [
        |          {
        |            "mode": "required",
        |            "name": "id",
        |            "type": "String"
        |          },
        |          {
        |            "mode": "nullable",
        |            "name": "refs",
        |            "type": "record",
        |            "fields": [
        |              {
        |                "mode": "required",
        |                "name": "refId",
        |                "type": "String"
        |              },
        |              {
        |                "mode": "nullable",
        |                "name": "refName",
        |                "type": "String"
        |              }
        |            ]
        |          }
        |        ]
        |      }
        |    }
      """.stripMargin.parseJson.convertTo[JSONSchema]

    val schemas = List(inSchema)

    val sources = List(
      KafkaTapDefinition(id = "kafkasource1", bootstrap = "0.0.0.0:9092", topic = "my-topic", group = Some("my-group"), defaultOffset = Some("latest")),
      KafkaTapDefinition(id = "kafkasource2", bootstrap = "127.0.0.1:9092", topic = "my-topic-out", group = None, defaultOffset = None)
    )

    val dag = List(
      DAGMapping(from = "in", to = "mapper"),
      DAGMapping(from = "mapper", to = "out")
    )

    val steps = List(
      SourceOp(`type` = "source", id = "in", name = "in", schema = "jsonschema1", tap = "kafkasource1"),
      TransformationOp(`type` = "transformation", id = "mapper", name = "mapper", op = "map", params = List(List("m => m.append('ts, Helper.fmt.print(Helper.Instant.now()))")), paramsEncoded = false),
      SinkOp(`type` = "sink", id = "out", name = "out", schema = None, tap = "kafkasource2")
    )

    val expectedConfig = Config(id = "schemaid", name = "schemaname", version = "version1",
      taps = sources, schemas = schemas, dag = dag, steps = steps)
    println()
    expectedConfig must equal (config)
  }
}