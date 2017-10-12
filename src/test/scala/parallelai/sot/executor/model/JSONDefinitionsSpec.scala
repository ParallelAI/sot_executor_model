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

      val schemas = List(AvroSchema(`type` = "avro", id = "avroschema1", version = "version2", definition = schema),
        BigQuerySchema(`type` = "bigquery", version = "version3", id = "bigqueryschema1", definition = schemaOut))

      val sources = List(PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource1", topic = "p2pout"),
        BigQueryTapDefinition(`type` = "bigquery", id = "bigquerysource1", dataset = "bigquerytest", table = "streaming_word_extract26"))

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )
      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "avroschema1", tap = "pubsubsource1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => BigQueryRow(m._1, m._2, Helper.fmt.print(Instant.now()))"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = Some("bigqueryschema1"), tap = "bigquerysource1")
      )

      val expectedConfig = Config(name = "schemaname", version = "version1", schemas = schemas,
        taps = sources, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

    "build pubsub with protobuf to bigquery config" in {
      val config = SOTMacroJsonConfig("psproto2bq-test.json")
      val schema =
        """
          |{
          |        "type": "protobufdefinition",
          |        "name": "MessageExtended",
          |        "fields": [
          |          {
          |            "mode": "required",
          |            "name": "user",
          |            "type": "string"
          |          },
          |          {
          |            "mode": "required",
          |            "name": "teamName",
          |            "type": "string"
          |          },
          |          {
          |            "mode": "required",
          |            "name": "score",
          |            "type": "int"
          |          },
          |          {
          |            "mode": "required",
          |            "name": "eventTime",
          |            "type": "long"
          |          },
          |          {
          |            "mode": "required",
          |            "name": "eventTimeStr",
          |            "type": "string"
          |          },
          |          {
          |            "mode": "required",
          |            "name": "count",
          |            "type": "int"
          |          }
          |        ]
          |      }
        """.stripMargin.parseJson.convertTo[ProtobufDefinition]

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

      val schemas = List(ProtobufSchema(`type` = "protobuf", id = "protoschema1", version = "version2", definition = schema),
        BigQuerySchema(`type` = "bigquery", version = "version3", id = "bigqueryschema1", definition = schemaOut))

      val sources = List(PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource1", topic = "p2pout"),
        BigQueryTapDefinition(`type` = "bigquery", id = "bigquerysource1", dataset = "bigquerytest", table = "streaming_word_extract26"))

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )
      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "protoschema1", tap = "pubsubsource1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => BigQueryRow(m._1, m._2, Helper.fmt.print(Instant.now()))"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = Some("bigqueryschema1"), tap = "bigquerysource1")
      )

      val expectedConfig = Config(name = "schemaname", version = "version1", schemas = schemas,
        taps = sources, dag = dag, steps = steps)
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

      val inSchema = AvroSchema(`type` = "avro", id = "avroschema1", version = "version2", definition = schema1)

      val schemas = List(inSchema)

      val sources = List(PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource1", topic = "p2pout"),
        BigTableTapDefinition(`type` = "bigtable", id = "bigtablesource1", instanceId = "bigtable-test",
          tableId = "bigquerytest", familyName = List("cf"), numNodes = 3))

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "avroschema1", tap = "pubsubsource1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => BigTableRecord(m._1, (\"cf\", \"counter\", m._2), (\"cf\", \"counter2\", m._2 * 1.225))"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = None, tap = "bigtablesource1")
      )

      val expectedConfig = Config(name = "schemaname", version = "version1",
        taps = sources, schemas = schemas, dag = dag, steps = steps)
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

      val inSchema = AvroSchema(`type` = "avro", id = "avroschema1", version = "version2", definition = schema1)
      val outSchema = AvroSchema(`type` = "avro", id = "avroschema2", version = "version2", definition = schema2)

      val schemas = List(inSchema, outSchema)

      val sources = List(
        PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource1", topic = "p2pin"),
        PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource2", topic = "p2pout")
      )

      val dag = List(
        DAGMapping(from = "in", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "avroschema1", tap = "pubsubsource1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => MessageExtended(m.user, m.teamName, m.score, m.eventTime, m.eventTimeStr, 1)"),
        SinkOp(`type` = "sink", name = "out", schema = Some("avroschema2"), tap = "pubsubsource2")
      )

      val expectedConfig = Config(name = "schemaname", version = "version1", taps = sources,
        schemas = schemas, dag = dag, steps = steps)
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

      val inSchema = AvroSchema(`type` = "avro", id = "avroschema1", version = "version2", definition = schema1)

      val schemas = List(inSchema)

      val sources = List(
        PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource1", topic = "p2pin"),
        DatastoreTapDefinition(`type` = "datastore", id = "datastoresource1", kind = "kind1")
      )

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", to = "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "avroschema1", tap = "pubsubsource1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => 'teamscores ->> m._1 :: 'score1 ->> m._2.toString :: 'score2 ->> (m._2 * 0.123) :: HNil"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = None, tap = "datastoresource1")
      )

      val expectedConfig = Config(name = "schemaname", version = "version1",
        taps = sources, schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

    "build pubsub to datastore with schema config" in {

      val config = SOTMacroJsonConfig("ps2ds-with-schema-test.json")

      val def1 =
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

      val def2 =
        """{
          |      "type": "datastoredefinition",
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

      val inSchema = AvroSchema(`type` = "avro", id = "avroschema1", version = "version2", definition = def1)
      val outSchema = DatastoreSchema(`type` = "datastore", id = "datastore1", version = "version3", definition = def2)

      val schemas = List(inSchema, outSchema)

      val sources = List(
        PubSubTapDefinition(`type` = "pubsub", id = "pubsubsource1", topic = "p2pin"),
        DatastoreTapDefinition(`type` = "datastore", id = "datastoresource1", kind = "kind1")
      )

      val dag = List(
        DAGMapping(from = "in", to = "filter"),
        DAGMapping(from = "filter", "mapper1"),
        DAGMapping(from = "mapper1", to = "sumByKey"),
        DAGMapping(from = "sumByKey", to = "mapper2"),
        DAGMapping(from = "mapper2", to = "out")
      )

      val steps = List(
        SourceOp(`type` = "source", name = "in", schema = "avroschema1", tap = "pubsubsource1"),
        TransformationOp(`type` = "transformation", name = "mapper1", op = "map", func = "m => (m.teamName, m.score.toInt)"),
        TransformationOp(`type` = "transformation", name = "filter", op = "filter", func = "m => m.score > 2"),
        TransformationOp(`type` = "transformation", name = "mapper2", op = "map", func = "m => OutSchema(m._1, m._2.toString, m._2 * 0.123)"),
        TransformationOp(`type` = "transformation", name = "sumByKey", op = "sumByKey", func = ""),
        SinkOp(`type` = "sink", name = "out", schema = Some("datastore1"), tap = "datastoresource1")
      )

      val expectedConfig = Config(name = "schemaname", version = "version1",
        taps = sources, schemas = schemas, dag = dag, steps = steps)
      expectedConfig should be(config)

    }

  }

}
