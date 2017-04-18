package fi.liikennevirasto.digiroad2.util

import fi.liikennevirasto.digiroad2.roadaddress.oracle.RoadAddressDAO
import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}

/**
  * A road consists of 1-2 tracks (fi: "ajorata"). 2 tracks are separated by a fence or grass for example.
  * Left and Right are relative to the advancing direction (direction of growing m values)
  */
sealed trait Track {
  def value: Int
}
object Track {
  val values = Set(Combined, RightSide, LeftSide, Unknown)

  def apply(intValue: Int): Track = {
    values.find(_.value == intValue).getOrElse(Unknown)
  }

  case object Combined extends Track { def value = 0 }
  case object RightSide extends Track { def value = 1 }
  case object LeftSide extends Track { def value = 2 }
  case object Unknown extends Track { def value = 99 }
}

/**
  * Road Side (fi: "puoli") tells the relation of an object and the track. For example, the side
  * where the bus stop is.
  */
sealed trait RoadSide {
  def value: Int
}
object RoadSide {
  val values = Set(Right, Left, Between, End, Middle, Across, Unknown)

  def apply(intValue: Int): RoadSide = {
    values.find(_.value == intValue).getOrElse(Unknown)
  }

  case object Right extends RoadSide { def value = 1 }
  case object Left extends RoadSide { def value = 2 }
  case object Between extends RoadSide { def value = 3 }
  case object End extends RoadSide { def value = 7 }
  case object Middle extends RoadSide { def value = 8 }
  case object Across extends RoadSide { def value = 9 }
  case object Unknown extends RoadSide { def value = 0 }
}

case class RoadAddress(municipalityCode: Option[String], road: Int, roadPart: Int, track: Track, mValue: Int, deviation: Option[Double])
class RoadAddressException(response: String) extends RuntimeException(response)

/**
  * A class to transform ETRS89-FI coordinates to road network addresses
  */

class GeometryTransform {
  // see page 16: http://www.liikennevirasto.fi/documents/20473/143621/tieosoitej%C3%A4rjestelm%C3%A4.pdf/

  def addressToCoords(road: Long, roadPart: Long, track: Track, mValue: Double) : Option[Point] = {
    val addresslist = RoadAddressDAO.getRoadAddress(RoadAddressDAO.withRoadAddress(road, roadPart, track.value, mValue)).headOption

    addresslist match {
      case Some(address) =>
        GeometryUtils.calculatePointFromLinearReference(address.geom, mValue-address.startAddrMValue)
      case _ => None
    }
  }

  def resolveAddressAndLocation(mValue: Double, linkId: Long, assetSideCode: Int, municipalityCode: Option[Int] = None, road: Option[Int] = None): (RoadAddress, RoadSide) = {

    val roadAddress = RoadAddressDAO.getRoadAddress(RoadAddressDAO.withLinkIdAndMeasure(linkId, mValue.toLong, mValue.toLong, road)).headOption

    val roadSide = roadAddress match {
      case Some(addrSide) if (addrSide.sideCode.value == assetSideCode) => RoadSide.Right //TowardsDigitizing //
      case Some(addrSide) if (addrSide.sideCode.value != assetSideCode) => RoadSide.Left //AgainstDigitizing //
      case _ => RoadSide.Unknown
    }

    val address = roadAddress match {
      case Some(addr) if (addr.track.eq(Track.Unknown)) => throw new RoadAddressException ("Invalid value for Track: %d".format( addr.track.value))
      case Some(addr) => RoadAddress(Some(municipalityCode.toString), addr.roadNumber.toInt, addr.roadPartNumber.toInt, addr.track, (addr.startAddrMValue + (mValue - addr.startMValue)).toInt, None)
      case None  => throw new RoadAddressException("No road address found")
    }

    (address, roadSide )
  }
}
