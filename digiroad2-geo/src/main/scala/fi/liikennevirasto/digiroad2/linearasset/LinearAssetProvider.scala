package fi.liikennevirasto.digiroad2.linearasset

import fi.liikennevirasto.digiroad2.asset.BoundingRectangle

case class Point(x: Double, y: Double)
case class LinearAsset(id: Long, points: Seq[Point])

trait LinearAssetProvider {
  def getAll(bounds: BoundingRectangle): Seq[LinearAsset]
}