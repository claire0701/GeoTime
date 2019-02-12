package geotime

import spray.json._

/** An object whose methods parse string representations as GeoJson */
object GeoJson {
  /** Parse a string as Json */
  def parse[T: JsonReader](json: String): T =
    json.parseJson.convertTo[T]
  /** Parse a file's contents as Json */
  def fromFile[T: JsonReader](path: String): T = {
    val src = scala.io.Source.fromFile(path)
    val txt =
      try {
        src.mkString
      } finally {
        src.close
      }
    parse[T](txt)
  }
}