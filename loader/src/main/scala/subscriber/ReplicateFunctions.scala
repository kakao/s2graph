package subscriber

import com.kakao.s2graph.core.GraphUtil
import play.api.libs.json.{JsObject, Json}

object ReplicateFunctions {
  def parseReplicationLog(line: String): Option[String] = {
    val sp = GraphUtil.split(line)
    val prop = if (sp.length > 6) sp(6) else "{}"
    val jsProps = Json.parse(prop)
    (jsProps \ "_rep_").asOpt[Boolean] match {
      case Some(b) =>
        if (b) {
          // drop
          None
        } else {
          // error
          throw new RuntimeException(s"Invalid replication flag: $line")
        }
      case None =>
        // replicate
        val newProps = jsProps.as[JsObject] ++ Json.obj("_rep_" -> true)
        val newSp = if (sp.length > 6) {
          sp(6) = newProps.toString()
          sp
        } else {
          sp ++ Array(newProps.toString())
        }
        Option(newSp.mkString("\t"))
    }
  }
}
