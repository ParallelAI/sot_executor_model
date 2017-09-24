package parallelai.sot.executor.model

import org.scalatest.{Matchers, WordSpec}
import parallelai.sot.executor.model.SOTMacroConfig._
import parallelai.sot.executor.model.SOTMacroJsonConfig._
import spray.json._

class JSONDefinitionsSpec extends WordSpec with Matchers {

  "SOTMacroConfig" should {

    "build pubsub to bigquery config" in {
      val config = SOTMacroJsonConfig("ps2bq-test.json")
      val schema =
        """
          |{
          |      "type": "record",
          |      "name": "MessageExtended",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "user",
          |          "type": "string",
          |          "doc": "Name of the user"
          |        },
          |        {
          |          "name": "teamName",
          |          "type": "string",
          |          "doc": "Name of the team"
          |        },
          |        {
          |          "name": "score",
          |          "type": "int",
          |          "doc": "User score"
          |        },
          |        {
          |          "name": "eventTime",
          |          "type": "long",
          |          "doc": "time when event created"
          |        },
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        },
          |        {
          |          "name": "count",
          |          "type": "int",
          |          "doc": "example count"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |    }
        """.stripMargin.parseJson.convertTo[AvroDefinition]

      val schemaOut =
        """
          |{
          |      "type": "bigquerydefinition",
          |      "name": "BigQueryRow",
          |      "fields": [
          |        {
          |          "mode": "REQUIRED",
          |          "name": "user",
          |          "type": "STRING"
          |        },
          |        {
          |          "mode": "REQUIRED",
          |          "name": "total_score",
          |          "type": "INTEGER"
          |        },
          |        {
          |          "mode": "REQUIRED",
          |          "name": "processing_time",
          |          "type": "STRING"
          |        }
          |      ]
          |    }
        """.stripMargin.stripMargin.parseJson.convertTo[BigQueryDefinition]

      val schemas = List(PubSubSchemaType(`type` = "pubsub", name = "pubsub1", serialization = "Avro", definition = schema, topic = "p2pout"),
        BigQuerySchemaType(`type` = "bigquery", name = "bigquery1", definition = schemaOut, dataset = "bigquerytest", table = "streaming_word_extract26"))

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )
      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "pubsub1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => BigQueryRow(m._1, m._2, Helper.fmt.print(Instant.now()))"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = "bigquery1")
      )

      val expectedConfig = Config(schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

    "build pubsub to bigtable config" in {
      val config = SOTMacroJsonConfig("ps2bt-test.json")

      val schema1 =
        """
          |{
          |      "type": "record",
          |      "name": "Message",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "user",
          |          "type": "string",
          |          "doc": "Name of the user"
          |        },
          |        {
          |          "name": "teamName",
          |          "type": "string",
          |          "doc": "Name of the team"
          |        },
          |        {
          |          "name": "score",
          |          "type": "int",
          |          "doc": "User score"
          |        },
          |        {
          |          "name": "eventTime",
          |          "type": "long",
          |          "doc": "time when event created"
          |        },
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |}
        """.stripMargin.parseJson.convertTo[AvroDefinition]

      val inSchema = PubSubSchemaType(`type` = "pubsub", name = "pubsub1", serialization = "Avro", definition = schema1, topic = "p2pin")
      val outSchema = BigTableSchemaType(`type` = "bigtable", name = "bigtable1" ,instanceId = "bigtable-test",
        tableId = "bigquerytest", familyName = List("cf"), numNodes = 3)

      val schemas = List(inSchema, outSchema)

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "pubsub1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => BigTableRecord(m._1, (\"cf\", \"counter\", m._2), (\"cf\", \"counter2\", m._2 * 1.225))"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = "bigtable1")
      )

      val expectedConfig = Config(schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

    "build pubsub to pubsub config" in {
      val config = SOTMacroJsonConfig("ps2ps-test.json")

      val schema1 =
        """
          |{
          |      "type": "record",
          |      "name": "Message",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "user",
          |          "type": "string",
          |          "doc": "Name of the user"
          |        },
          |        {
          |          "name": "teamName",
          |          "type": "string",
          |          "doc": "Name of the team"
          |        },
          |        {
          |          "name": "score",
          |          "type": "int",
          |          "doc": "User score"
          |        },
          |        {
          |          "name": "eventTime",
          |          "type": "long",
          |          "doc": "time when event created"
          |        },
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |    }
        """.stripMargin.parseJson.convertTo[AvroDefinition]

      val schema2 =
        """
          |{
          |      "type": "record",
          |      "name": "MessageExtended",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "user",
          |          "type": "string",
          |          "doc": "Name of the user"
          |        },
          |        {
          |          "name": "teamName",
          |          "type": "string",
          |          "doc": "Name of the team"
          |        },
          |        {
          |          "name": "score",
          |          "type": "int",
          |          "doc": "User score"
          |        },
          |        {
          |          "name": "eventTime",
          |          "type": "long",
          |          "doc": "time when event created"
          |        },
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        },
          |        {
          |          "name": "count",
          |          "type": "int",
          |          "doc": "example count"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |    }
        """.stripMargin.stripMargin.parseJson.convertTo[AvroDefinition]

      val inSchema = PubSubSchemaType(`type` = "pubsub", name = "pubsub1", serialization = "Avro", definition = schema1, topic = "p2pin")
      val outSchema = PubSubSchemaType(`type` = "pubsub", name = "pubsub2", serialization = "Avro", definition = schema2, topic = "p2pout")

      val schemas = List(inSchema, outSchema)

      val dag = List(
        DAGMapping(from = "in", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "pubsub1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => MessageExtended(m.user, m.teamName, m.score, m.eventTime, m.eventTimeStr, 1)"),
        SinkOp(`type` = "sink", name = "out", schema = "pubsub2")
      )

      val expectedConfig = Config(schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

    "build pubsub to datastore config" in {
      val config = SOTMacroJsonConfig("ps2ds-test.json")

      val schema1 =
        """
          |{
          |      "type": "record",
          |      "name": "Message",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "user",
          |          "type": "string",
          |          "doc": "Name of the user"
          |        },
          |        {
          |          "name": "teamName",
          |          "type": "string",
          |          "doc": "Name of the team"
          |        },
          |        {
          |          "name": "score",
          |          "type": "int",
          |          "doc": "User score"
          |        },
          |        {
          |          "name": "eventTime",
          |          "type": "long",
          |          "doc": "time when event created"
          |        },
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |    }
        """.stripMargin.stripMargin.parseJson.convertTo[AvroDefinition]

      val inSchema = PubSubSchemaType(`type` = "pubsub", serialization = "Avro", name = "pubsub1", definition = schema1, topic = "p2pin")
      val outSchema = DatastoreSchemaType(`type` = "datastore", name = "datastore1", kind = "kind1", definition =  None)

      val schemas = List(inSchema, outSchema)

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "pubsub1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => 'teamscores ->> m._1 :: 'score1 ->> m._2.toString :: 'score2 ->> (m._2 * 0.123) :: HNil"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = "datastore1")
      )

      val expectedConfig = Config(schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

    "build pubsub to datastore with schema config" in {

      val config = SOTMacroJsonConfig("ps2ds-with-schema-test.json")

      val schema1 =
        """
          |{
          |      "type": "record",
          |      "name": "Message",
          |      "namespace": "parallelai.sot.avro",
          |      "fields": [
          |        {
          |          "name": "user",
          |          "type": "string",
          |          "doc": "Name of the user"
          |        },
          |        {
          |          "name": "teamName",
          |          "type": "string",
          |          "doc": "Name of the team"
          |        },
          |        {
          |          "name": "score",
          |          "type": "int",
          |          "doc": "User score"
          |        },
          |        {
          |          "name": "eventTime",
          |          "type": "long",
          |          "doc": "time when event created"
          |        },
          |        {
          |          "name": "eventTimeStr",
          |          "type": "string",
          |          "doc": "event time string for debugging"
          |        }
          |      ],
          |      "doc": "A basic schema for storing user records"
          |    }
        """.stripMargin.stripMargin.parseJson.convertTo[AvroDefinition]

      val schema2 =
        """{
          |      "name": "OutSchema",
          |      "fields": [
          |        {
          |          "name": "teamscores",
          |          "type": "String"
          |        },
          |        {
          |          "name": "score1",
          |          "type": "String"
          |        },
          |        {
          |          "name": "score2",
          |          "type": "Double"
          |        }
          |      ]
          |    }
        """.stripMargin.stripMargin.parseJson.convertTo[DatastoreDefinition]

      val inSchema = PubSubSchemaType(`type` = "pubsub", serialization = "Avro", name = "pubsub1", definition = schema1, topic = "p2pin")
      val outSchema = DatastoreSchemaType(`type` = "datastore", name = "datastore1", kind = "kind1", definition = Some(schema2))

      val schemas = List(inSchema, outSchema)

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "pubsub1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => OutSchema(m._1, m._2.toString, m._2 * 0.123)"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = "datastore1")
      )

      val expectedConfig = Config(schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }


  }

}
