package parallelai.sot.executor.model

import java.io.{File, FileInputStream, InputStream}

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Seq


object SOTMacroConfig {

  sealed trait Definition {
    def `type`: String

    def name: String
  }

  sealed trait Schema {
    def `type`: String

    def id: String

    def version: String

    def definition: Definition
  }

  sealed trait TapDefinition {
    def `type`: String

    def id: String
  }

  /** Schema Definitions **/
  case class AvroDefinition(`type`: String, name: String, namespace: String, fields: JsArray) extends Definition

  case class BigQueryDefinition(`type`: String, name: String, fields: JsArray) extends Definition

  case class DatastoreDefinitionField(`type`: String, name: String)

  case class DatastoreDefinition(`type`: String, name: String, fields: List[DatastoreDefinitionField]) extends Definition

  case class ProtobufDefinition(`type`: String, name: String, fields: List[ProtobufDefinitionField]) extends Definition

  case class ProtobufDefinitionField(mode: String, `type`: String, name: String)

  case class ByteArrayDefinition(`type`: String, name: String) extends Definition


  /** Schema Types **/
  case class AvroSchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  case class ProtobufSchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  case class BigQuerySchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  case class DatastoreSchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  case class ByteArraySchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  /** Source Types **/
  case class PubSubTapDefinition(`type`: String, id: String, topic: String) extends TapDefinition

  case class GoogleStoreTapDefinition(`type`: String, id: String, bucket: String, blob: String) extends TapDefinition

  case class BigQueryTapDefinition(`type`: String, id: String, dataset: String, table: String) extends TapDefinition

  case class BigTableTapDefinition(`type`: String, id: String, instanceId: String, tableId: String, familyName: List[String], numNodes: Int) extends TapDefinition

  case class DatastoreTapDefinition(`type`: String, id: String, kind: String) extends TapDefinition

  case class DAGMapping(from: String, to: String) extends Topology.Edge[String]

  case class Config(name: String, version: String, schemas: List[Schema], taps: List[TapDefinition],
                    dag: List[DAGMapping], steps: List[OpType]) {

    def parseDAG(): Topology[String, DAGMapping] = {
      val vertices = (dag.map(_.from) ++ dag.map(_.to)).distinct
      Topology.createTopology(vertices, dag)
    }

  }

  sealed trait OpType {
    def `type`: String

    def name: String
  }

  case class TransformationOp(`type`: String, name: String, op: String, func: String) extends OpType

  case class SourceOp(`type`: String, name: String, schema: String, tap: String) extends OpType

  case class SinkOp(`type`: String, name: String, schema: Option[String], tap: String) extends OpType

}

object SOTMacroJsonConfig {

  import SOTMacroConfig._

  implicit val avroDefinitionFormat = jsonFormat4(AvroDefinition)
  implicit val protobufDefinitionFieldFormat = jsonFormat3(ProtobufDefinitionField)
  implicit val protobufDefinitionFormat = jsonFormat3(ProtobufDefinition)
  implicit val bytearrayDefinitionFormat = jsonFormat2(ByteArrayDefinition)
  implicit val bigQueryDefinitionFormat = jsonFormat3(BigQueryDefinition)
  implicit val datastoreDefinitionFieldFormat = jsonFormat2(DatastoreDefinitionField)
  implicit val datastoreDefinitionFormat = jsonFormat3(DatastoreDefinition)

  implicit object DefinitionJsonFormat extends RootJsonFormat[Definition] {

    def write(c: Definition): JsValue =
      c match {
        case s: AvroDefinition => s.toJson
        case s: ProtobufDefinition => s.toJson
        case s: ByteArrayDefinition => s.toJson
        case s: BigQueryDefinition => s.toJson
        case s: DatastoreDefinition => s.toJson
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
        case Seq(JsString(typ)) if typ == "protobufdefinition" =>
          value.asJsObject.getFields("type", "name", "fields") match {
            case Seq(JsString(typ), JsString(name), JsArray(fields)) =>
              val fl = fields.map(_.convertTo[ProtobufDefinitionField]).toList
              ProtobufDefinition(`type` = typ, name = name, fields = fl)
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

  implicit val pubSubTapDefinition = jsonFormat3(PubSubTapDefinition)
  implicit val googleStoreTapDefinition = jsonFormat4(GoogleStoreTapDefinition)
  implicit val bigQueryTapDefinition = jsonFormat4(BigQueryTapDefinition)
  implicit val bigTableTapDefinition = jsonFormat6(BigTableTapDefinition)
  implicit val datastoreTapDefinition = jsonFormat3(DatastoreTapDefinition)

  implicit object SourceJsonFormat extends RootJsonFormat[TapDefinition] {

    def write(s: TapDefinition): JsValue =
      s match {
        case j: PubSubTapDefinition => j.toJson
        case j: GoogleStoreTapDefinition => j.toJson
        case j: BigQueryTapDefinition => j.toJson
        case j: BigTableTapDefinition => j.toJson
        case j: DatastoreTapDefinition => j.toJson
      }

    def read(value: JsValue): TapDefinition = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "pubsub" =>
          value.asJsObject.getFields("type", "id", "topic") match {
            case Seq(JsString(objType), JsString(name), JsString(topic)) =>
              PubSubTapDefinition(`type` = objType, id = name, topic = topic)
            case _ => deserializationError("Pubsub source expected")
          }
        case Seq(JsString(typ)) if typ == "googlestore" =>
          value.asJsObject.getFields("type", "id", "bucket", "blob") match {
            case Seq(JsString(objType), JsString(name), JsString(bucket), JsString(blob)) =>
              GoogleStoreTapDefinition(`type` = objType, id = name, bucket = bucket, blob = blob)
            case _ => deserializationError("Pubsub source expected")
          }
        case Seq(JsString(typ)) if typ == "bigquery" =>
          value.asJsObject.getFields("type", "id", "dataset", "table") match {
            case Seq(JsString(objType), JsString(name), JsString(dataset), JsString(table)) =>
              BigQueryTapDefinition(`type` = objType, id = name, dataset = dataset, table = table)
            case _ => deserializationError("BigQuery source expected")
          }
        case Seq(JsString(typ)) if typ == "bigtable" =>
          value.asJsObject.getFields("type", "id", "instanceId", "tableId", "familyName", "numNodes") match {
            case Seq(JsString(objType), JsString(name), JsString(instanceId), JsString(tableId), familyName, JsNumber(numNodes)) =>
              val fn = familyName.convertTo[List[String]]
              BigTableTapDefinition(`type` = objType, id = name, instanceId = instanceId, tableId = tableId, familyName = fn, numNodes = numNodes.toInt)
            case _ => deserializationError("BigTable source expected")
          }
        case Seq(JsString(typ)) if typ == "datastore" =>
          value.asJsObject.getFields("type", "id", "kind") match {
            case Seq(JsString(objType), JsString(name), JsString(kind)) =>
              DatastoreTapDefinition(`type` = objType, id = name, kind = kind)
            case _ => deserializationError("Datastore source expected")
          }
        case _ => deserializationError("Source expected")
      }
    }
  }

  implicit val avroSchemaFormat = jsonFormat4(AvroSchema)
  implicit val protobufSchemaFormat = jsonFormat4(ProtobufSchema)
  implicit val bigQuerySchemaFormat = jsonFormat4(BigQuerySchema)
  implicit val datastoreSchemaFormat = jsonFormat4(DatastoreSchema)
  implicit val byteArraySchemaFormat = jsonFormat4(ByteArraySchema)

  implicit object SchemaJsonFormat extends RootJsonFormat[Schema] {

    def write(c: Schema): JsValue =
      c match {
        case s: AvroSchema => s.toJson
        case s: ProtobufSchema => s.toJson
        case s: BigQuerySchema => s.toJson
        case s: DatastoreSchema => s.toJson
        case s: ByteArraySchema => s.toJson
      }

    def read(value: JsValue): Schema = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "avro" => {
          value.asJsObject.getFields("type", "id", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              AvroSchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Avro schema expected")
          }
        }
        case Seq(JsString(typ)) if typ == "protobuf" => {
          value.asJsObject.getFields("type", "id", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              ProtobufSchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Protobuf schema expected")
          }
        }
        case Seq(JsString(typ)) if typ == "bigquery" => {
          value.asJsObject.getFields("type", "id", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              BigQuerySchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("BigQuery schema expected")
          }
        }
        case Seq(JsString(typ)) if typ == "datastore" => {
          value.asJsObject.getFields("type", "id", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              DatastoreSchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Datastore schema expected")
          }
        }
        case Seq(JsString(typ)) if typ == "bytearray" => {
          value.asJsObject.getFields("type", "id", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              ByteArraySchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("ByteArray schema expected")
          }
        }
        case _ => deserializationError("Schema expected")
      }
    }
  }

  implicit val transformationOpFormat = jsonFormat4(TransformationOp)
  implicit val sinkOpFormat = jsonFormat4(SinkOp)
  implicit val sourceOpFormat = jsonFormat4(SourceOp)

  implicit object OpJsonFormat extends RootJsonFormat[OpType] {

    def write(c: OpType): JsValue = {
      c match {
        case s: TransformationOp => s.toJson
        case s: SinkOp => s.toJson
        case s: SourceOp => s.toJson
      }
    }

    def read(value: JsValue): OpType = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "source" => {
          value.asJsObject.getFields("type", "name", "schema", "source") match {
            case Seq(JsString(objType), JsString(name), JsString(schema), JsString(source)) =>
              SourceOp(`type` = objType, name = name, schema = schema, tap = source)
            case _ => deserializationError("SourceOp type expected")
          }
        }
        case Seq(JsString(typ)) if typ == "sink" => {
          value.asJsObject.getFields("type", "name", "source", "schema") match {
            case Seq(JsString(objType), JsString(name), JsString(source), JsString(schema)) =>
              SinkOp(`type` = objType, name = name, schema = Some(schema), tap = source)
            case Seq(JsString(objType), JsString(name), JsString(source)) =>
              SinkOp(`type` = objType, name = name, schema = None, tap = source)
            case _ => deserializationError("SinkOp type expected")
          }
        }
        case Seq(JsString(typ)) if typ == "transformation" => {
          value.asJsObject.getFields("type", "name", "op", "func") match {
            case Seq(JsString(objType), JsString(name), JsString(op), JsString(func)) =>
              TransformationOp(`type` = objType, name = name, op = op, func = func)
            case _ => deserializationError("TransformationOp type expected")
          }
        }
        case _ => deserializationError("SchemaType expected")
      }
    }
  }

  implicit val dagFormat = jsonFormat2(DAGMapping)
  implicit val configFormat = jsonFormat6(Config)

  def apply(fileName: String): Config = {
    val stream: InputStream = getClass.getResourceAsStream("/" + fileName)
    val source = scala.io.Source.fromInputStream(stream)
    val lines = try source.mkString finally source.close()
    val config = lines.parseJson.convertTo[Config]
    ConfigHelper.validate(config)
    config
  }
}