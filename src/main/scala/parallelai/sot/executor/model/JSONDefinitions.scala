package parallelai.sot.executor.model

import java.io.InputStream

import parallelai.sot.executor.model.SOTMacroConfig.DatastoreSchemaType
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Seq


object SOTMacroConfig {

  sealed trait Definition {
    def `type`: String

    def name: String
  }

  sealed trait SchemaType {
    def `type`: String

    def name: String

  }

  sealed trait SchemaTypeWithDefinition extends SchemaType {

    def definition : Definition

  }


  /** Schema Definitions **/
  case class AvroDefinition(`type`: String, name: String, namespace: String, fields: JsArray) extends Definition

  case class BigQueryDefinition(`type`: String, name: String, fields: JsArray) extends Definition

  case class DatastoreDefinitionField(`type`: String, name: String)

  case class DatastoreDefinition(`type`: String, name: String, fields: List[DatastoreDefinitionField]) extends Definition


  /** Schema Types **/
  case class PubSubSchemaType(`type`: String, name: String, serialization: String, definition: AvroDefinition, topic: String) extends SchemaTypeWithDefinition

  case class BigQuerySchemaType(`type`: String, name: String, definition: BigQueryDefinition, dataset: String, table: String) extends SchemaTypeWithDefinition

  case class BigTableSchemaType(`type`: String, name: String, instanceId: String, tableId: String, familyName: List[String], numNodes: Int) extends SchemaType

  case class DatastoreSchemaType(`type`: String, name: String, kind: String, definition: DatastoreDefinition) extends SchemaTypeWithDefinition

  case class DatastoreSchemalessSchemaType(`type`: String, name: String, kind: String) extends SchemaType

  case class DAGMapping(from: String, to: String) extends Topology.Edge[String]

  case class Config(schemas: List[SchemaType], dag: List[DAGMapping], steps: List[OpType]) {

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

  case class SourceOp(`type`: String, name: String, schema: String) extends OpType

  case class SinkOp(`type`: String, name: String, schema: String) extends OpType

}

object SOTMacroJsonConfig {

  import SOTMacroConfig._

  implicit val avroDefinitionFormat = jsonFormat4(AvroDefinition)
  implicit val bigQueryDefinitionFormat = jsonFormat3(BigQueryDefinition)
  implicit val datastoreDefinitionFieldFormat = jsonFormat2(DatastoreDefinitionField)
  implicit val datastoreDefinitionFormat = jsonFormat3(DatastoreDefinition)

  implicit val pubsubSchemaFormat = jsonFormat5(PubSubSchemaType)
  implicit val bigQueryFormat = jsonFormat5(BigQuerySchemaType)
  implicit val bigTableFormat = jsonFormat6(BigTableSchemaType)
  implicit val datastoreSchemaFormat = jsonFormat4(DatastoreSchemaType)
  implicit val datastoreSchemalessSchemaTypeFormat = jsonFormat3(DatastoreSchemalessSchemaType)

  implicit object SchemaJsonFormat extends RootJsonFormat[SchemaType] {

    def write(c: SchemaType): JsValue =
      c match {
        case s: PubSubSchemaType => s.toJson
        case s: BigQuerySchemaType => s.toJson
        case s: BigTableSchemaType => s.toJson
        case s: DatastoreSchemaType => s.toJson
        case s: DatastoreSchemalessSchemaType => s.toJson
      }

    def parsePubsubDefinition(definition: JsValue): AvroDefinition = {
      definition.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "record" =>
          definition.asJsObject.getFields("type", "name", "namespace", "fields") match {
            case Seq(JsString(objType), JsString(name), JsString(namespace), fields) =>
              AvroDefinition(`type` = objType, name = name, namespace = namespace, fields = fields.asInstanceOf[JsArray])
            case _ => deserializationError("AvroDefinition expected")
          }
        case _ => deserializationError("Unsupported definition for pubsub")
      }
    }

    def parseBigQueryDefinition(definition: JsValue): BigQueryDefinition = {
      definition.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "bigquerydefinition" =>
          definition.asJsObject.getFields("type", "name", "fields") match {
            case Seq(JsString(objType), JsString(name), fields) =>
              BigQueryDefinition(`type` = objType, name = name, fields = fields.asInstanceOf[JsArray])
            case _ => deserializationError("BigQueryDefinition is expected")
          }
        case _ => deserializationError("Unsupported definition for bigquery")
      }
    }

    def parseDatastoreDefinition(definition: JsValue): DatastoreDefinition = {
      definition.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "datastoredefinition" =>
          definition.asJsObject.getFields("type", "name", "fields") match {
            case Seq(JsString(typ), JsString(name), JsArray(fields)) =>
              val fl = fields.map(_.convertTo[DatastoreDefinitionField]).toList
              DatastoreDefinition(`type` = typ, name = name, fields = fl)
            case _ => deserializationError("DatastoreDefinition is expected")
          }
        case _ => deserializationError("unsupported definition for datastore")
      }
    }


    def read(value: JsValue): SchemaType = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "pubsub" => {
          value.asJsObject.getFields("type", "serialization", "name", "definition", "topic") match {
            case Seq(JsString(objType), JsString(serialization), JsString(name), definition, JsString(topic)) =>
              PubSubSchemaType(`type` = objType, serialization = serialization, name = name, definition = parsePubsubDefinition(definition), topic = topic)
            case _ => deserializationError("PubSub expected")
          }
        }
        case Seq(JsString(typ)) if typ == "bigquery" => {
          value.asJsObject.getFields("type", "name", "definition", "dataset", "table") match {
            case Seq(JsString(objType), JsString(name), definition, JsString(dataset), JsString(table)) =>
              BigQuerySchemaType(`type` = objType, name = name, definition = parseBigQueryDefinition(definition), table = table, dataset = dataset)
            case _ => deserializationError("BigQuery expected")
          }
        }
        case Seq(JsString(typ)) if typ == "bigtable" => {
          value.asJsObject.getFields("type", "name", "instanceId", "tableId", "familyName", "numNodes") match {
            case Seq(JsString(objType), JsString(name), JsString(instanceId), JsString(tableId), familyName, JsNumber(numNodes)) =>
              val fn = familyName.convertTo[List[String]]
              BigTableSchemaType(`type` = objType, name = name, instanceId = instanceId, tableId = tableId, familyName = fn, numNodes = numNodes.toInt)
            case _ => deserializationError("BigTable expected")
          }

        }
        case Seq(JsString(typ)) if typ == "datastore" => {
          value.asJsObject.getFields("type", "name", "definition", "kind") match {
            case Seq(JsString(objType), JsString(name), definition, JsString(kind)) =>
              DatastoreSchemaType(`type` = objType, name = name, kind = kind, definition = parseDatastoreDefinition(definition))
            case Seq(JsString(objType), JsString(name), JsString(kind)) =>
              DatastoreSchemalessSchemaType(`type` = objType, name = name, kind = kind)
            case _ => deserializationError("Datastore expected")
          }
        }
        case _ => deserializationError("SchemaType expected")
      }
    }
  }

  implicit val transformationOpFormat = jsonFormat4(TransformationOp)
  implicit val sinkOpFormat = jsonFormat3(SinkOp)
  implicit val sourceOpFormat = jsonFormat3(SourceOp)

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
          value.asJsObject.getFields("type", "name", "schema") match {
            case Seq(JsString(objType), JsString(name), JsString(schema)) =>
              SourceOp(`type` = objType, name = name, schema = schema)
            case _ => deserializationError("SourceOp type expected")
          }
        }
        case Seq(JsString(typ)) if typ == "sink" => {
          value.asJsObject.getFields("type", "name", "schema") match {
            case Seq(JsString(objType), JsString(name), JsString(schema)) =>
              SinkOp(`type` = objType, name = name, schema = schema)
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
  implicit val configFormat = jsonFormat3(Config)

  def apply(fileName: String): Config = {
    val stream: InputStream = getClass.getResourceAsStream("/" + fileName)
    val source = scala.io.Source.fromInputStream(stream)
    val lines = try source.mkString finally source.close()
    val config = lines.parseJson.convertTo[Config]

    config
  }
}