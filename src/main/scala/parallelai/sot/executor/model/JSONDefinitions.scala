package parallelai.sot.executor.model

import java.io.InputStream
import spray.json.DefaultJsonProtocol._
import spray.json._

object SOTMacroConfig {
  sealed trait Definition {
    def `type`: String

    def name: String
  }

  sealed trait Schema {
    def `type`: String

    def id: String

    def name: String

    def version: String

    def definition: Definition
  }

  sealed trait TapDefinitionType {
    def `type`: String
  }

  sealed trait TapDefinition extends TapDefinitionType {
    def id: String
  }

  sealed trait LookupDefinition {
    def id: String

    def schema: String

    def tap: String
  }

  /** Lookup Definitions **/
  case class DatastoreLookupDefinition(id: String, schema: String, tap: String) extends LookupDefinition

  /** Schema Definitions **/
  case class AvroDefinition(`type`: String, name: String, namespace: String, fields: JsArray) extends Definition

  case class BigQueryDefinition(`type`: String, name: String, fields: JsArray) extends Definition

  case class DatastoreDefinitionField(`type`: String, name: String)

  case class DatastoreDefinition(`type`: String, name: String, fields: List[DatastoreDefinitionField]) extends Definition

  case class ProtobufDefinition(`type`: String, name: String, schemaBase64: String) extends Definition

  case class JSONDefinitionField(`type`: String, name: String, mode: String, fields: Option[List[JSONDefinitionField]])

  case class JSONDefinition(`type`: String, name: String, fields: List[JSONDefinitionField]) extends Definition

  case class ByteArrayDefinition(`type`: String, name: String) extends Definition

  case class SeqDefinition(`type`: String = "sequence", name: String = "sequence") extends Definition

  /** Schema Types **/
  case class AvroSchema(`type`: String, id: String, name: String, version: String, definition: Definition) extends Schema

  case class ProtobufSchema(`type`: String, id: String, name: String, version: String, definition: Definition) extends Schema

  case class BigQuerySchema(`type`: String, id: String, name: String, version: String, definition: Definition) extends Schema

  case class DatastoreSchema(`type`: String, id: String, name: String, version: String, definition: Definition) extends Schema

  case class JSONSchema(`type`: String, id: String, name: String, version: String, definition: Definition) extends Schema

  case class ByteArraySchema(`type`: String, id: String, name: String, version: String, definition: Definition) extends Schema

  /** Source Types **/
  case class PubSubTapDefinition(`type`: String, id: String, topic: String, managedSubscription: Option[Boolean], timestampAttribute: Option[String], idAttribute: Option[String]) extends TapDefinition

  case class GoogleStoreTapDefinition(`type`: String, id: String, bucket: String, blob: String) extends TapDefinition

  // WriteDisposition: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY
  // CreateDisposition: CREATE_NEVER | CREATE_IF_NEEDED
  case class BigQueryTapDefinition(`type`: String, id: String, dataset: String, table: String, writeDisposition: Option[String], createDisposition: Option[String]) extends TapDefinition

  case class BigTableTapDefinition(`type`: String, id: String, instanceId: String, tableId: String, familyName: List[String], numNodes: Int) extends TapDefinition

  case class DatastoreTapDefinition(`type`: String, id: String, kind: String, dedupeStrategy: DedupeStrategy = DedupeStrategy.NONE, allowPartialUpdates: Boolean = false) extends TapDefinition

  object SeqTapDefinition extends TapDefinitionType {
    val `type` = "sequence"
  }

  case class SeqTapDefinition[T <: Product](id: String = "sequenceTapDefinition", content: Seq[T]) extends TapDefinition {
    def `type`: String = SeqTapDefinition.`type`
  }

  object KafkaTapDefinitionType extends TapDefinitionType {
    val `type` = "kafka"
  }

  case class KafkaTapDefinition(id: String, bootstrap: String, topic: String, group: Option[String], defaultOffset: Option[String], autoCommit: Option[Boolean]) extends TapDefinition {
    def `type`: String = KafkaTapDefinitionType.`type`
  }

  case class DAGMapping(from: String, to: String) extends Topology.Edge[String]

  case class Config(id: String, name: String, version: String, schemas: List[Schema], lookups: List[LookupDefinition] = Nil, taps: List[TapDefinition], dag: List[DAGMapping], steps: List[OpType]) {
    def parseDAG(): Topology[String, DAGMapping] = {
      val vertices = (dag.map(_.from) ++ dag.map(_.to)).distinct
      Topology.createTopology(vertices, dag)
    }
  }

  sealed trait OpType {
    def `type`: String

    def id: String

    def name: String
  }

  case class TransformationOp(`type`: String, id: String, name: String, op: String, params: Seq[Seq[String]], paramsEncoded: Boolean) extends OpType

  case class TFPredictOp(`type`: String, id: String, name: String, modelBucket: String, modelPath: String, fetchOps: Seq[String], inFn: String, outFn: String) extends OpType

  case class SourceOp(`type`: String, id: String, name: String, schema: String, tap: String) extends OpType

  case class SinkOp(`type`: String, id: String, name: String, schema: Option[String], tap: String) extends OpType
}

object SOTMacroJsonConfig extends SOTMacroJsonConfig

trait SOTMacroJsonConfig {
  import SOTMacroConfig._

  implicit val avroDefinitionFormat: RootJsonFormat[AvroDefinition] =
    jsonFormat4(AvroDefinition)

  implicit val protobufDefinitionFormat: RootJsonFormat[ProtobufDefinition] =
    jsonFormat3(ProtobufDefinition)

  implicit val bigQueryDefinitionFormat: RootJsonFormat[BigQueryDefinition] =
    jsonFormat3(BigQueryDefinition)

  implicit val datastoreDefinitionFieldFormat: RootJsonFormat[DatastoreDefinitionField] =
    jsonFormat2(DatastoreDefinitionField)

  implicit val datastoreDefinitionFormat: RootJsonFormat[DatastoreDefinition] =
    jsonFormat3(DatastoreDefinition)

  implicit val jsonDefinitionFieldFormat: JsonFormat[JSONDefinitionField] =
    lazyFormat(jsonFormat(JSONDefinitionField, "type", "name", "mode", "fields"))

  implicit val jsonDefinitionFormat: RootJsonFormat[JSONDefinition] =
    jsonFormat3(JSONDefinition)

  implicit val bytearrayDefinitionFormat: RootJsonFormat[ByteArrayDefinition] =
    jsonFormat2(ByteArrayDefinition)

  implicit val definitionJsonFormat: RootJsonFormat[Definition] = new RootJsonFormat[Definition] {
    def write(c: Definition): JsValue = c match {
      case s: AvroDefinition => s.toJson
      case s: ProtobufDefinition => s.toJson
      case s: ByteArrayDefinition => s.toJson
      case s: BigQueryDefinition => s.toJson
      case s: DatastoreDefinition => s.toJson
      case s: JSONDefinition => s.toJson
    }

    def read(value: JsValue): Definition = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "record" =>
          value.asJsObject.getFields("type", "name", "namespace", "fields") match {
            case Seq(JsString(objType), JsString(name), JsString(namespace), fields) =>
              AvroDefinition(`type` = objType, name = name, namespace = namespace, fields = fields.asInstanceOf[JsArray])
            case _ => deserializationError("AvroDefinition expected")
          }

        case Seq(JsString(typ)) if typ == "bigquerydefinition" =>
          value.asJsObject.getFields("type", "name", "fields") match {
            case Seq(JsString(objType), JsString(name), fields) =>
              BigQueryDefinition(`type` = objType, name = name, fields = fields.asInstanceOf[JsArray])
            case _ => deserializationError("BigQueryDefinition is expected")
          }

        case Seq(JsString(typ)) if typ == "datastoredefinition" =>
          value.asJsObject.getFields("type", "name", "fields") match {
            case Seq(JsString(typ), JsString(name), JsArray(fields)) =>
              val fl = fields.map(_.convertTo[DatastoreDefinitionField]).toList
              DatastoreDefinition(`type` = typ, name = name, fields = fl)
            case _ => deserializationError("DatastoreDefinition is expected")
          }

        case Seq(JsString(typ)) if typ == "jsondefinition" =>
          value.asJsObject.getFields("type", "name", "mode", "fields") match {
            case Seq(JsString(typ), JsString(name), JsArray(fields)) =>
              val fl = fields.map(_.convertTo[JSONDefinitionField]).toList
              JSONDefinition(`type` = typ, name = name, fields = fl)
            case _ => deserializationError("JSONDefinition is expected")
          }

        case Seq(JsString(typ)) if typ == "protobufdefinition" =>
          value.asJsObject.getFields("type", "name", "schemaBase64") match {
            case Seq(JsString(typ), JsString(name), JsString(schemaBase64)) =>
              ProtobufDefinition(`type` = typ, name = name, schemaBase64 = schemaBase64)
            case _ => deserializationError("ProtobufDefinition is expected")
          }

        case Seq(JsString(typ)) if typ == "bytearraydefinition" =>
          value.asJsObject.getFields("type", "name") match {
            case Seq(JsString(typ), JsString(name)) =>
              ByteArrayDefinition(`type` = typ, name = name)
            case _ => deserializationError("ProtobufDefinition is expected")
          }

        case _ => deserializationError("Unsupported definition")
      }
    }
  }

  implicit val pubSubTapDefinition: RootJsonFormat[PubSubTapDefinition] =
    jsonFormat6(PubSubTapDefinition)

  implicit val googleStoreTapDefinition: RootJsonFormat[GoogleStoreTapDefinition] =
    jsonFormat4(GoogleStoreTapDefinition)

  implicit val bigQueryTapDefinition: RootJsonFormat[BigQueryTapDefinition] =
    jsonFormat6(BigQueryTapDefinition)

  implicit val bigTableTapDefinition: RootJsonFormat[BigTableTapDefinition] =
    jsonFormat6(BigTableTapDefinition)

  implicit val dedupeStrategyFormat: JsonFormat[DedupeStrategy] = new JsonFormat[DedupeStrategy] {
    def read(json: JsValue): DedupeStrategy = DedupeStrategy.valueOf(json.convertTo[String])

    def write(obj: DedupeStrategy): JsValue = obj.name().toJson
  }

  implicit val datastoreTapDefinition: RootJsonFormat[DatastoreTapDefinition] =
    jsonFormat5(DatastoreTapDefinition)

  implicit val kafkaTapDefinition: RootJsonFormat[KafkaTapDefinition] =
    jsonFormat6(KafkaTapDefinition)

  implicit val sourceJsonFormat: RootJsonFormat[TapDefinition] = new RootJsonFormat[TapDefinition] {
    def write(s: TapDefinition): JsValue =
      s match {
        case j: PubSubTapDefinition => j.toJson
        case j: GoogleStoreTapDefinition => j.toJson
        case j: BigQueryTapDefinition => j.toJson
        case j: BigTableTapDefinition => j.toJson
        case j: DatastoreTapDefinition => j.toJson
        case j: KafkaTapDefinition => s.toJson
      }

    def read(value: JsValue): TapDefinition = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "pubsub" =>
          value.asJsObject.getFields("type", "id", "topic") match {
            case Seq(JsString(objType), JsString(id), JsString(topic)) =>
              PubSubTapDefinition(`type` = objType, id = id, topic = topic,
                managedSubscription = value.asJsObject.fields.get("managedSubscription").map(_.convertTo[Boolean]),
                timestampAttribute = value.asJsObject.fields.get("timestampAttribute").map(_.convertTo[String]),
                idAttribute = value.asJsObject.fields.get("idAttribute").map(_.convertTo[String]))
            case _ => deserializationError("Pubsub source expected")
          }

        case Seq(JsString(typ)) if typ == "googlestore" =>
          value.asJsObject.getFields("type", "id", "bucket", "blob") match {
            case Seq(JsString(objType), JsString(id), JsString(bucket), JsString(blob)) =>
              GoogleStoreTapDefinition(`type` = objType, id = id, bucket = bucket, blob = blob)
            case _ => deserializationError("GoogleStore source expected")
          }

        case Seq(JsString(typ)) if typ == "bigquery" =>
          value.asJsObject.getFields("type", "id", "dataset", "table") match {
            case Seq(objType, name, dataset, table) =>
              BigQueryTapDefinition(`type` = objType.convertTo[String], id = name.convertTo[String],
                dataset = dataset.convertTo[String], table = table.convertTo[String], writeDisposition = value.asJsObject.fields.get("writeDisposition").map(_.convertTo[String]),
                createDisposition = value.asJsObject.fields.get("createDisposition").map(_.convertTo[String]))
            case _ => deserializationError("BigQuery source expected")
          }

        case Seq(JsString(typ)) if typ == "bigtable" =>
          value.asJsObject.getFields("type", "id", "instanceId", "tableId", "familyName", "numNodes") match {
            case Seq(JsString(objType), JsString(id), JsString(instanceId), JsString(tableId), familyName, JsNumber(numNodes)) =>
              val fn = familyName.convertTo[List[String]]
              BigTableTapDefinition(`type` = objType, id = id, instanceId = instanceId, tableId = tableId, familyName = fn, numNodes = numNodes.toInt)
            case _ => deserializationError("BigTable source expected")
          }

        case Seq(JsString(typ)) if typ == "datastore" =>
          value.asJsObject.getFields("type", "id", "kind", "dedupeStrategy", "allowPartialUpdates") match {
            case Seq(JsString(objType), JsString(id), JsString(kind), dedupeStrategy, JsBoolean(allowPartialUpdates)) =>
              DatastoreTapDefinition(`type` = objType, id = id, kind = kind, dedupeStrategy = dedupeStrategy.convertTo[DedupeStrategy], allowPartialUpdates = allowPartialUpdates)
            case Seq(JsString(objType), JsString(id), JsString(kind), dedupeStrategy) =>
              DatastoreTapDefinition(`type` = objType, id = id, kind = kind, dedupeStrategy = dedupeStrategy.convertTo[DedupeStrategy])
            case _ => deserializationError("Datastore source expected")
          }

        case Seq(JsString(typ)) if typ == "googlestore" =>
          value.asJsObject.getFields("type", "id", "bucket", "blob") match {
            case Seq(JsString(objType), JsString(id), JsString(bucket), JsString(blob)) =>
              GoogleStoreTapDefinition(`type` = objType, id = id, bucket = bucket, blob = blob)
            case _ => deserializationError("GoogleStore source expected")
          }

        case Seq(JsString(typ)) if typ == "kafka" =>
          value.asJsObject.getFields("id", "bootstrap", "topic") match {
            case Seq(JsString(id), JsString(bootstrap), JsString(topic)) =>
              KafkaTapDefinition(id = id, bootstrap = bootstrap, topic = topic,
                group = value.asJsObject.fields.get("group").map(_.convertTo[String]),
                defaultOffset = value.asJsObject.fields.get("defaultOffset").map(_.convertTo[String]),
                autoCommit = value.asJsObject.fields.get("autoCommit").map(_.convertTo[Boolean]))
            case _ => deserializationError("Kafka source expected")
          }

        case _ => deserializationError("Source expected")
      }
    }
  }

  implicit val avroSchemaFormat: RootJsonFormat[AvroSchema] =
    jsonFormat5(AvroSchema)

  implicit val protobufSchemaFormat: RootJsonFormat[ProtobufSchema] =
    jsonFormat5(ProtobufSchema)

  implicit val bigQuerySchemaFormat: RootJsonFormat[BigQuerySchema] =
    jsonFormat5(BigQuerySchema)

  implicit val datastoreSchemaFormat: RootJsonFormat[DatastoreSchema] =
    jsonFormat5(DatastoreSchema)

  implicit val jsonSchemaFormat: RootJsonFormat[JSONSchema] =
    jsonFormat5(JSONSchema)

  implicit val byteArraySchemaFormat: RootJsonFormat[ByteArraySchema] =
    jsonFormat5(ByteArraySchema)

  implicit val schemaJsonFormat: RootJsonFormat[Schema] = new RootJsonFormat[Schema] {
    def write(c: Schema): JsValue =
      c match {
        case s: AvroSchema => s.toJson
        case s: ProtobufSchema => s.toJson
        case s: BigQuerySchema => s.toJson
        case s: DatastoreSchema => s.toJson
        case s: JSONSchema => s.toJson
        case s: ByteArraySchema => s.toJson
      }

    def read(value: JsValue): Schema = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "avro" =>
          value.asJsObject.getFields("type", "id", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(version), definition) =>
              AvroSchema(`type` = objType, id = id, name = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Avro schema expected")
          }

        case Seq(JsString(typ)) if typ == "protobuf" =>
          value.asJsObject.getFields("type", "id", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(version), definition) =>
              ProtobufSchema(`type` = objType, id = id, name = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Protobuf schema expected")
          }

        case Seq(JsString(typ)) if typ == "bigquery" =>
          value.asJsObject.getFields("type", "id", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(version), definition) =>
              BigQuerySchema(`type` = objType, id = id, name = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("BigQuery schema expected")
          }

        case Seq(JsString(typ)) if typ == "datastore" =>
          value.asJsObject.getFields("type", "id", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(version), definition) =>
              DatastoreSchema(`type` = objType, id = id, name = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Datastore schema expected")
          }

        case Seq(JsString(typ)) if typ == "json" =>
          value.asJsObject.getFields("type", "id", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(version), definition) =>
              JSONSchema(`type` = objType, id = id, name = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("JSON schema expected")
          }

        case Seq(JsString(typ)) if typ == "bytearray" =>
          value.asJsObject.getFields("type", "id", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(version), definition) =>
              ByteArraySchema(`type` = objType, id = id, name = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("ByteArray schema expected")
          }

        case _ => deserializationError("Schema expected")
      }
    }
  }

  implicit val tfPredictOpFormat: RootJsonFormat[TFPredictOp] =
    jsonFormat8(TFPredictOp)

  implicit val transformationOpFormat: RootJsonFormat[TransformationOp] =
    jsonFormat6(TransformationOp)

  implicit val sinkOpFormat: RootJsonFormat[SinkOp] =
    jsonFormat5(SinkOp)

  implicit val sourceOpFormat: RootJsonFormat[SourceOp] =
    jsonFormat5(SourceOp)

  implicit val opJsonFormat: RootJsonFormat[OpType] = new RootJsonFormat[OpType] {
    def write(c: OpType): JsValue = {
      c match {
        case s: TFPredictOp => s.toJson
        case s: TransformationOp => s.toJson
        case s: SinkOp => s.toJson
        case s: SourceOp => s.toJson
      }
    }

    def read(value: JsValue): OpType = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "source" =>
          value.asJsObject.getFields("type", "id", "name", "schema", "tap") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(schema), JsString(tap)) =>
              SourceOp(`type` = objType, id = id, name = name, schema = schema, tap = tap)
            case _ => deserializationError("SourceOp type expected")
          }

        case Seq(JsString(typ)) if typ == "sink" =>
          value.asJsObject.getFields("type", "id", "name", "schema", "tap") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(schema), JsString(tap)) =>
              SinkOp(`type` = objType, id = id, name = name, schema = Option(schema), tap = tap)

            case Seq(JsString(objType), JsString(id), JsString(name), JsString(tap)) =>
              SinkOp(`type` = objType, id = id, name = name, schema = None, tap = tap)

            case _ => deserializationError("SinkOp type expected")
          }

        case Seq(JsString(typ)) if typ == "transformation" =>
          value.asJsObject.getFields("type", "id", "name", "op", "params", "paramsEncoded") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(op), JsArray(params), JsBoolean(paramsEncoded)) =>
              TransformationOp(`type` = objType, id = id, name = name, op = op, params = params.map(_.convertTo[Seq[String]]), paramsEncoded = paramsEncoded)
            case _ => deserializationError("TransformationOp type expected")
          }

        case Seq(JsString(typ)) if typ == "tfpredict" =>
          value.asJsObject.getFields("type", "id", "name", "modelBucket", "modelPath", "fetchOps", "inFn", "outFn") match {
            case Seq(JsString(objType), JsString(id), JsString(name), JsString(modelBucket), JsString(modelPath),
              JsArray(fetchOps), JsString(inFn), JsString(outFn)) =>
              val fOps = fetchOps.map(_.convertTo[String])
              TFPredictOp(`type` = objType, id = id, name = name, modelBucket = modelBucket, modelPath = modelPath, fetchOps = fOps,
                inFn = inFn, outFn = outFn)
            case _ => deserializationError("tfpredict type expected")
          }

        case _ => deserializationError("SchemaType expected")
      }
    }
  }

  implicit val lookupDefinitionFormat: RootJsonFormat[LookupDefinition] = new RootJsonFormat[LookupDefinition] {
    implicit val datastoreFormat: RootJsonFormat[DatastoreLookupDefinition] = jsonFormat3(DatastoreLookupDefinition)

    def read(json: JsValue): DatastoreLookupDefinition =
      datastoreFormat.read(json)

    def write(obj: LookupDefinition): JsValue = obj match {
      case d: DatastoreLookupDefinition => d.toJson
    }
  }

  implicit val dagFormat: RootJsonFormat[DAGMapping] =
    jsonFormat2(DAGMapping)

  implicit val configFormat: RootJsonFormat[Config] =
    jsonFormat8(Config)

  def apply(fileName: String): Config = {
    val stream: InputStream = getClass.getResourceAsStream("/" + fileName)
    val source = scala.io.Source.fromInputStream(stream)
    val lines = try source.mkString finally source.close()
    val config = lines.parseJson.convertTo[Config]
    ConfigHelper.validate(config)
    config
  }
}