package pers


import com.google.gson.{JsonElement, JsonParser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

package object machi {

    val extractMessageAsMapUdf = udf[Option[Map[String, String]], String](extractMessageAsMap)

    def extractMessageAsMap(value:String):Option[Map[String, String]] = {
        val value_ = Option(value).getOrElse(return None)

        val valueJsonObject = JsonParser.parseString(value_).getAsJsonObject
        val message = valueJsonObject.get("message").getAsString
        val messageJsonObject = JsonParser.parseString(message).getAsJsonObject
        Some(json2Map(messageJsonObject))
    }

    def json2Map(root: JsonElement) = {
        val toFilled = new mutable.HashMap[String, String]
        fillMapFromJson(toFilled, root, "")
        toFilled.toMap
    }

    def fillMapFromJson(recordMap: mutable.HashMap[String, String], root: JsonElement, prefix: String): Unit = {
        val rootObject = root.getAsJsonObject

        val it = rootObject.entrySet().iterator()

        while (it.hasNext) {
            val node = it.next()

            val k = node.getKey
            val v = node.getValue

            val prefix_ = if ("".equals(prefix)) node.getKey else s"$prefix.$k"

            if (!v.isJsonNull) {
                if (v.isJsonPrimitive) {
                    recordMap.+=((prefix_, v.getAsString))
                } else if (v.isJsonArray) {
                    recordMap.+=((prefix_, v.toString))
                } else {
                    fillMapFromJson(recordMap, v, prefix_)
                }
            }
        }
    }
}
