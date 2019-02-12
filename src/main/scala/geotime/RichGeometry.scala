package geotime

import com.esri.core.geometry.Geometry
import com.esri.core.geometry.GeometryEngine
import com.esri.core.geometry.SpatialReference
import scala.language.implicitConversions


class RichGeometry(val geometry: Geometry,
                   val spatialReference: SpatialReference = SpatialReference.create(4326)) {

  def area2D(): Double = geometry.calculateArea2D()

  def contains(other: Geometry): Boolean = {
    GeometryEngine.contains(geometry, other, spatialReference)
  }

  def distance(other: Geometry): Double = {
    GeometryEngine.distance(geometry, other, spatialReference)
  }
}

object RichGeometry {
  implicit def wrapRichGeo(g :Geometry): RichGeometry = {
    new RichGeometry(g)
  }
}