package parallelai.sot.executor.model

import java.io.InputStream

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Seq


object SOTMacroConfig {

  sealed trait SchemaType

  case class PubSubSchemaType(`type`: String, serialization: String, name: String, schema: JsObject, topic: String) extends SchemaType

  case class BigQuerySchemaType(`type`: String, name: String, schema: JsObject, dataset: String, table: String) extends SchemaType

  case class BigTableSchemaType(`type`: String, instanceId: String, tableId: String, familyName: List[String], numNodes: Int) extends SchemaType

  case class DatastoreSchemaField(name: String, `type`: String)
  case class DatastoreSchema(name: String, fields: List[DatastoreSchemaField])
  case class DatastoreSchemaType(`type`: String, schema: Option[DatastoreSchema]) extends SchemaType

  case class DAGMapping(from: String, to: String) extends Topology.Edge[String]
  case class Config(in: SchemaType, out: SchemaType, dag: List[DAGMapping], steps: List[Op]) {

    def parseDAG() : Topology[String, DAGMapping] = {
      val vertices = (dag.map(_.from) ++ dag.map(_.to)).distinct
      Topology.createTopology(vertices, dag)
    }

  }

  case class Op(`type`: String, name: String, op: String, func: String)

}

object SOTMacroJsonConfig {

  import SOTMacroConfig._

  implicit val avroSchemaFormat = jsonFormat5(PubSubSchemaType)
  implicit val bigQueryFormat = jsonFormat5(BigQuerySchemaType)
  implicit val bigTableFormat = jsonFormat5(BigTableSchemaType)

  implicit val datastoreSchemaFieldFormat = jsonFormat2(DatastoreSchemaField)
  implicit val datastoreSchemaFormat = jsonFormat2(DatastoreSchema)
  implicit val datastoreFormat = jsonFormat2(DatastoreSchemaType)

  implicit object SchemaJsonFormat extends RootJsonFormat[SchemaType] {

    def write(c: SchemaType): JsValue =
      c match {
        case s: PubSubSchemaType => s.toJson
        case s: BigQuerySchemaType => s.toJson
        case s: BigTableSchemaType => s.toJson
        case s: DatastoreSchemaType => s.toJson
      }

    def read(value: JsValue): SchemaType = {
      value.asJsObject.getFields("type") match {
        case Seq(JsString(typ)) if typ == "PubSub" => {
          value.asJsObject.getFields("type", "serialization", "name", "schema", "topic") match {
            case Seq(JsString(objType), JsString(serialization), JsString(name), schema, JsString(topic)) =>
              PubSubSchemaType(`type` = objType, serialization = serialization, name = name, schema = schema.asJsObject, topic = topic)
            case _ => deserializationError("PubSub expected")
          }
        }
        case Seq(JsString(typ)) if typ == "BigQuery" => {
          value.asJsObject.getFields("type", "name", "schema", "dataset", "table") match {
            case Seq(JsString(objType), JsString(name), schema, JsString(dataset), JsString(table)) =>
              BigQuerySchemaType(`type` = objType, name = name, schema = schema.asJsObject, table = table, dataset = dataset)
            case _ => deserializationError("BigQuery expected")
          }
        }
        case Seq(JsString(typ)) if typ == "BigTable" => {
          value.asJsObject.getFields("type", "instanceId", "tableId", "familyName", "numNodes") match {
            case Seq(JsString(objType), JsString(instanceId), JsString(tableId), familyName, JsNumber(numNodes)) =>
              val fn = familyName.convertTo[List[String]]
              BigTableSchemaType(`type` = objType, instanceId = instanceId, tableId = tableId, familyName = fn, numNodes = numNodes.toInt)
            case _ => deserializationError("BigTable expected")
          }
        }
        case Seq(JsString(typ)) if typ == "Datastore" => {
          value.asJsObject.getFields("type", "schema") match {
            case Seq(JsString(objType), JsObject(schema)) =>
              DatastoreSchemaType(`type` = objType, schema = Some(schema.toJson.convertTo[DatastoreSchema]))
            case Seq(JsString(objType)) =>
              DatastoreSchemaType(`type` = objType, schema = None)
            case _ => deserializationError("Datastore expected")
          }
        }
        case _ => deserializationError("SchemaType expected")
      }
    }
  }

  implicit val opFormat = jsonFormat4(Op)
  implicit val dagFormat = jsonFormat2(DAGMapping)
  implicit val configFormat = jsonFormat4(Config)

  def apply(fileName: String): Config = {
    val stream: InputStream = getClass.getResourceAsStream("/" + fileName)
    val source = scala.io.Source.fromInputStream(stream)
    val lines = try source.mkString finally source.close()
    val config = lines.parseJson.convertTo[Config]

    config
  }
}