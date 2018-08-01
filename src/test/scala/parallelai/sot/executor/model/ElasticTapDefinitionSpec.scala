package parallelai.sot.executor.model

import org.scalatest.{ FlatSpec, MustMatchers }
import parallelai.sot.executor.model.SOTMacroConfig._
import parallelai.sot.executor.model.SOTMacroJsonConfig._
import spray.json._

class ElasticTapDefinitionSpec extends FlatSpec with MustMatchers {
  "ElasticTapDefinition" should "be of expected type" in {

    val tapDefinition = ElasticTapDefinition("my-id", "my-bootstrap", 9300, "local", "my-index", "my-index", 1)

    tapDefinition.`type` mustEqual ElasticTapDefinitionType.`type`
  }

  it should "be correctly initialised as source and sink" in {

    val config = SOTMacroJsonConfig("kf2elastic-json-test.json")

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
      ElasticTapDefinition("elastictap1", "0.0.0.0", 9300, "local", "my-index1", "my-type1", 1),
      ElasticTapDefinition("elastictap2", "0.0.0.0", 9300, "local", "my-index2", "my-type2", 1)
    )

    val dag = List(
      DAGMapping(from = "in", to = "mapper"),
      DAGMapping(from = "mapper", to = "out")
    )

    val steps = List(
      SourceOp(`type` = "source", id = "in", name = "in", schema = "jsonschema1", tap = "elastictap1"),
      TransformationOp(`type` = "transformation", id = "mapper", name = "mapper", op = "map", params = List(List("m => m.append('ts, Helper.fmt.print(Helper.Instant.now()))")), paramsEncoded = false),
      SinkOp(`type` = "sink", id = "out", name = "out", schema = None, tap = "elastictap2")
    )

    val expectedConfig = Config(id = "schemaid", name = "schemaname", version = "version1",
      taps = sources, schemas = schemas, lookups = Nil, dag = dag, steps = steps)
    println()
    expectedConfig must equal(config)
  }
}