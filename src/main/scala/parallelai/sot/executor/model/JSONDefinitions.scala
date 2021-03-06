package parallelai.sot.executor.model

import java.io.InputStream

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

  sealed trait Source {
    def `type`: String

    def id: String
  }

  /** Schema Definitions **/
  case class AvroDefinition(`type`: String, name: String, namespace: String, fields: JsArray) extends Definition

  case class BigQueryDefinition(`type`: String, name: String, fields: JsArray) extends Definition

  case class DatastoreDefinitionField(`type`: String, name: String)

  case class DatastoreDefinition(`type`: String, name: String, fields: List[DatastoreDefinitionField]) extends Definition

  /** Schema Types **/
  case class AvroSchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  case class BigQuerySchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  case class DatastoreSchema(`type`: String, id: String, version: String, definition: Definition) extends Schema

  /** Source Types **/
  case class PubSubSource(`type`: String, id: String, topic: String) extends Source

  case class BigQuerySource(`type`: String, id: String, dataset: String, table: String) extends Source

  case class BigTableSource(`type`: String, id: String, instanceId: String, tableId: String, familyName: List[String], numNodes: Int) extends Source

  case class DatastoreSource(`type`: String, id: String, kind: String) extends Source

  case class DAGMapping(from: String, to: String) extends Topology.Edge[String]

  case class Config(name: String, version: String, schemas: List[Schema], sources: List[Source],
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

  case class SourceOp(`type`: String, name: String, schema: String, source: String) extends OpType

  case class SinkOp(`type`: String, name: String, schema: Option[String], source: String) extends OpType

}

object SOTMacroJsonConfig {

  import SOTMacroConfig._

  implicit val avroDefinitionFormat = jsonFormat4(AvroDefinition)
  implicit val bigQueryDefinitionFormat = jsonFormat3(BigQueryDefinition)
  implicit val datastoreDefinitionFieldFormat = jsonFormat2(DatastoreDefinitionField)
  implicit val datastoreDefinitionFormat = jsonFormat3(DatastoreDefinition)

  implicit object DefinitionJsonFormat extends RootJsonFormat[Definition] {

    def write(c: Definition): JsValue =
      c match {
        case s: AvroDefinition => s.toJson
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
        case _ => deserializationError("Unsupported definition")
      }
    }
  }

  implicit val pubSubSourceDefinition = jsonFormat3(PubSubSource)
  implicit val bigQuerySourceFormat = jsonFormat4(BigQuerySource)
  implicit val bigTableSourceFormat = jsonFormat6(BigTableSource)
  implicit val datastoreSource = jsonFormat3(DatastoreSource)

  implicit object SourceJsonFormat extends RootJsonFormat[Source] {

    def write(s: Source): JsValue =
      s match {
        case j: PubSubSource => j.toJson
        case j: BigQuerySource => j.toJson
        case j: BigTableSource => j.toJson
        case j: DatastoreSource => j.toJson
      }

    def read(value: JsValue): Source = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "pubsub" =>
          value.asJsObject.getFields("type", "name", "topic") match {
            case Seq(JsString(objType), JsString(name), JsString(topic)) =>
              PubSubSource(`type` = objType, id = name, topic = topic)
            case _ => deserializationError("Pubsub source expected")
          }
        case Seq(JsString(typ)) if typ == "bigquery" =>
          value.asJsObject.getFields("type", "name", "dataset", "table") match {
            case Seq(JsString(objType), JsString(name), JsString(dataset), JsString(table)) =>
              BigQuerySource(`type` = objType, id = name, dataset = dataset, table = table)
            case _ => deserializationError("BigQuery source expected")
          }
        case Seq(JsString(typ)) if typ == "bigtable" =>
          value.asJsObject.getFields("type", "name", "instanceId", "tableId", "familyName", "numNodes") match {
            case Seq(JsString(objType), JsString(name), JsString(instanceId), JsString(tableId), familyName, JsNumber(numNodes)) =>
              val fn = familyName.convertTo[List[String]]
              BigTableSource(`type` = objType, id = name, instanceId = instanceId, tableId = tableId, familyName = fn, numNodes = numNodes.toInt)
            case _ => deserializationError("BigTable source expected")
          }
        case Seq(JsString(typ)) if typ == "datastore" =>
          value.asJsObject.getFields("type", "name", "kind") match {
            case Seq(JsString(objType), JsString(name), JsString(kind)) =>
              DatastoreSource(`type` = objType, id = name, kind = kind)
            case _ => deserializationError("Datastore source expected")
          }
        case _ => deserializationError("Source expected")
      }
    }
  }

  implicit val avroSchemaFormat = jsonFormat4(AvroSchema)
  implicit val bigQuerySchemaFormat = jsonFormat4(BigQuerySchema)
  implicit val datastoreSchemaFormat = jsonFormat4(DatastoreSchema)

  implicit object SchemaJsonFormat extends RootJsonFormat[Schema] {

    def write(c: Schema): JsValue =
      c match {
        case s: AvroSchema => s.toJson
        case s: BigQuerySchema => s.toJson
        case s: DatastoreSchema => s.toJson
      }

    def read(value: JsValue): Schema = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "avro" => {
          value.asJsObject.getFields("type", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              AvroSchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Avro schema expected")
          }
        }
        case Seq(JsString(typ)) if typ == "bigquery" => {
          value.asJsObject.getFields("type", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              BigQuerySchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("BigQuery schema expected")
          }
        }
        case Seq(JsString(typ)) if typ == "datastore" => {
          value.asJsObject.getFields("type", "name", "version", "definition") match {
            case Seq(JsString(objType), JsString(name), JsString(version), definition) =>
              DatastoreSchema(`type` = objType, id = name, version = version, definition = definition.convertTo[Definition])
            case _ => deserializationError("Datastore schema expected")
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
              SourceOp(`type` = objType, name = name, schema = schema, source = source)
            case _ => deserializationError("SourceOp type expected")
          }
        }
        case Seq(JsString(typ)) if typ == "sink" => {
          value.asJsObject.getFields("type", "name", "source", "schema") match {
            case Seq(JsString(objType), JsString(name), JsString(source), JsString(schema)) =>
              SinkOp(`type` = objType, name = name, schema = Some(schema), source = source)
            case Seq(JsString(objType), JsString(name), JsString(source)) =>
              SinkOp(`type` = objType, name = name, schema = None, source = source)
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