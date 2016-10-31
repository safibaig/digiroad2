package fi.liikennevirasto.viite.dao

import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import fi.liikennevirasto.digiroad2.asset.SideCode
import fi.liikennevirasto.digiroad2.asset.SideCode.{AgainstDigitizing, BothDirections, TowardsDigitizing, Unknown}
import fi.liikennevirasto.digiroad2.oracle.MassQuery
import fi.liikennevirasto.digiroad2.util.Track
import fi.liikennevirasto.viite.dao.CalibrationCode._
import fi.liikennevirasto.viite.RoadType
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import slick.jdbc.{GetResult, StaticQuery => Q}
import com.github.tototoshi.slick.MySQLJodaSupport._
import fi.liikennevirasto.viite.RoadType._
import fi.liikennevirasto.viite.model.Anomaly
import org.joda.time.DateTime

//JATKUVUUS (1 = Tien loppu, 2 = epäjatkuva (esim. vt9 välillä Akaa-Tampere), 3 = ELY:n raja, 4 = Lievä epäjatkuvuus (esim kiertoliittymä), 5 = jatkuva)

sealed trait Discontinuity {
  def value: Int
}
object Discontinuity {
  val values = Set(EndOfRoad, Discontinuous, ChangingELYCode, MinorDiscontinuity, Continuous)

  def apply(intValue: Int): Discontinuity = {
    values.find(_.value == intValue).getOrElse(Continuous)
  }

  case object EndOfRoad extends Discontinuity { def value = 1 }
  case object Discontinuous extends Discontinuity { def value = 2 }
  case object ChangingELYCode extends Discontinuity { def value = 3 }
  case object MinorDiscontinuity extends Discontinuity { def value = 4 }
  case object Continuous extends Discontinuity { def value = 5 }
}

sealed trait CalibrationCode {
  def value: Int
}
object CalibrationCode {
  val values = Set(No, AtEnd, AtBeginning, AtBoth)

  def apply(intValue: Int): CalibrationCode = {
    values.find(_.value == intValue).getOrElse(No)
  }

  case object No extends CalibrationCode { def value = 0 }
  case object AtEnd extends CalibrationCode { def value = 1 }
  case object AtBeginning extends CalibrationCode { def value = 2 }
  case object AtBoth extends CalibrationCode { def value = 3 }
}
case class CalibrationPoint(linkId: Long, segmentMValue: Double, addressMValue: Long)

case class RoadAddress(id: Long, roadNumber: Long, roadPartNumber: Long, track: Track, discontinuity: Discontinuity,
                       startAddrMValue: Long, endAddrMValue: Long, startDate: DateTime, endDate: Option[DateTime],
                       linkId: Long, startMValue: Double, endMValue: Double,
                       calibrationPoints: (Option[CalibrationPoint], Option[CalibrationPoint]) = (None, None), floating: Boolean = false)

case class MissingRoadAddress(linkId: Long, startAddrMValue: Option[Long], endAddrMValue: Option[Long],
                              roadType: RoadType, roadNumber: Option[Long], roadPartNumber: Option[Long],
                              startMValue: Option[Double], endMValue: Option[Double], anomaly: Anomaly)

object RoadAddressDAO {

  private def logger = LoggerFactory.getLogger(getClass)

  private def calibrations(calibrationCode: CalibrationCode, linkId: Long, startMValue: Double, endMValue: Double,
                           startAddrMValue: Long, endAddrMValue: Long, sideCode: SideCode): (Option[CalibrationPoint], Option[CalibrationPoint]) = {
    sideCode match {
      case BothDirections => (None, None) // Invalid choice
      case TowardsDigitizing => calibrations(calibrationCode, linkId, 0.0, endMValue-startMValue, startAddrMValue, endAddrMValue)
      case AgainstDigitizing => calibrations(calibrationCode, linkId, endMValue-startMValue, 0.0, startAddrMValue, endAddrMValue)
      case Unknown => (None, None)  // Invalid choice
    }
  }

  private def calibrations(calibrationCode: CalibrationCode, linkId: Long, segmentStartMValue: Double, segmentEndMValue: Double,
                           startAddrMValue: Long, endAddrMValue: Long): (Option[CalibrationPoint], Option[CalibrationPoint]) = {
    calibrationCode match {
      case No => (None, None)
      case AtEnd => (None, Some(CalibrationPoint(linkId, segmentEndMValue, endAddrMValue)))
      case AtBeginning => (Some(CalibrationPoint(linkId, segmentStartMValue, startAddrMValue)), None)
      case AtBoth => (Some(CalibrationPoint(linkId, segmentStartMValue, startAddrMValue)),
        Some(CalibrationPoint(linkId, segmentEndMValue, endAddrMValue)))
    }
  }
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  def dateTimeParse(string: String) = {
    try {
      DateTime.parse(string, formatter)
    } catch {
      case ex: Exception => null
    }
  }

  def optDateTimeParse(string: String): Option[DateTime] = {
    try {
      if (string==null || string == "")
        None
      else
        Some(DateTime.parse(string, formatter))
    } catch {
      case ex: Exception => return None
    }
  }

  def fetchByLinkId(linkIds: Set[Long]): List[RoadAddress] = {
    if (linkIds.size > 1000) {
      return fetchByLinkIdMassQuery(linkIds)
    }
    val linkIdString = linkIds.mkString(",")
    val where = linkIds.isEmpty match {
      case true => return List()
      case false => s""" where pos.link_id in ($linkIdString)"""
    }
    val query =
      s"""
        select ra.id, ra.road_number, ra.road_part_number, ra.track_code,
        ra.discontinuity, ra.start_addr_m, ra.end_addr_m, pos.link_id, pos.start_measure, pos.end_measure,
        pos.side_code,
        ra.start_date, ra.end_date, ra.created_by, ra.created_date, ra.CALIBRATION_POINTS
        from road_address ra
        join lrm_position pos on ra.lrm_position_id = pos.id
        $where AND floating='0'
      """
    val tuples = Q.queryNA[(Long, Long, Long, Int, Int, Long, Long, Long, Double, Double, Int,
      String, String, String, String, Int)](query).list
    tuples.map {
      case (id, roadNumber, roadPartNumber, track, discontinuity, startAddrMValue, endAddrMValue,
      linkId, startMValue, endMValue, sideCode, startDate, endDate, createdBy, createdDate, calibrationCode) =>
        RoadAddress(id, roadNumber, roadPartNumber, Track.apply(track), Discontinuity.apply(discontinuity), startAddrMValue, endAddrMValue, dateTimeParse(startDate), optDateTimeParse(endDate), linkId, startMValue, endMValue, calibrations(CalibrationCode.apply(calibrationCode), linkId, startMValue, endMValue, startAddrMValue, endAddrMValue, SideCode.apply(sideCode)))
    }

  }

  def fetchPartsByRoadNumbers(roadNumbers: Seq[(Int, Int)], coarse: Boolean = false): List[RoadAddress] = {
    val filter = roadNumbers.map(n => "road_number >= " + n._1 + " and road_number <= " + n._2)
      .mkString("(", ") OR (", ")")
    //.foldLeft("")((c, r) => c + ") OR (" + r)
    val where = roadNumbers.isEmpty match {
      case true => return List()
      case false => s""" where track_code in (0,1) AND $filter"""
    }
    val coarseWhere = coarse match {
      case true => " AND calibration_points != 0"
      case false => ""
    }
    val query =
      s"""
        select ra.id, ra.road_number, ra.road_part_number, ra.track_code,
        ra.discontinuity, ra.start_addr_m, ra.end_addr_m, pos.link_id, pos.start_measure, pos.end_measure,
        pos.side_code,
        ra.start_date, ra.end_date, ra.created_by, ra.created_date, ra.CALIBRATION_POINTS
        from road_address ra
        join lrm_position pos on ra.lrm_position_id = pos.id
        $where $coarseWhere AND floating='0'
      """
    val tuples = Q.queryNA[(Long, Long, Long, Int, Int, Long, Long, Long, Double, Double, Int,
      String, String, String, String, Int)](query).list
    tuples.map {
      case (id, roadNumber, roadPartNumber, track, discontinuity, startAddrMValue, endAddrMValue,
      linkId, startMValue, endMValue, sideCode, startDate, endDate, createdBy, createdDate, calibrationCode) =>
        RoadAddress(id, roadNumber, roadPartNumber, Track.apply(track), Discontinuity.apply(discontinuity), startAddrMValue, endAddrMValue, dateTimeParse(startDate), optDateTimeParse(endDate), linkId, startMValue, endMValue, calibrations(CalibrationCode.apply(calibrationCode), linkId, startMValue, endMValue, startAddrMValue, endAddrMValue, SideCode.apply(sideCode)))
    }

  }

  def fetchByLinkIdMassQuery(linkIds: Set[Long]): List[RoadAddress] = {
    MassQuery.withIds(linkIds) {
      idTableName =>
        val query =
          s"""
        select ra.id, ra.road_number, ra.road_part_number, ra.track_code,
        ra.discontinuity, ra.start_addr_m, ra.end_addr_m, pos.link_id, pos.start_measure, pos.end_measure,
        pos.side_code,
        ra.start_date, ra.end_date, ra.created_by, ra.created_date, ra.CALIBRATION_POINTS
        from road_address ra
        join lrm_position pos on ra.lrm_position_id = pos.id
        join $idTableName i on i.id = pos.link_id
        where floating='0'
      """
        val tuples = Q.queryNA[(Long, Long, Long, Int, Int, Long, Long, Long, Double, Double, Int,
          String, String, String, String, Int)](query).list
        tuples.map {
          case (id, roadNumber, roadPartNumber, track, discontinuity, startAddrMValue, endAddrMValue,
          linkId, startMValue, endMValue, sideCode, startDate, endDate, createdBy, createdDate, calibrationCode) =>
            RoadAddress(id, roadNumber, roadPartNumber, Track.apply(track), Discontinuity.apply(discontinuity), startAddrMValue, endAddrMValue, dateTimeParse(startDate), optDateTimeParse(endDate), linkId, startMValue, endMValue, calibrations(CalibrationCode.apply(calibrationCode), linkId, startMValue, endMValue, startAddrMValue, endAddrMValue, SideCode.apply(sideCode)))
        }
    }
  }

  def fetchByRoadPart(roadNumber: Long, roadPartNumber: Long) = {
    val query =
      s"""
        select ra.id, ra.road_number, ra.road_part_number, ra.track_code,
        ra.discontinuity, ra.start_addr_m, ra.end_addr_m, pos.link_id, pos.start_measure, pos.end_measure,
        pos.side_code,
        ra.start_date, ra.end_date, ra.created_by, ra.created_date, ra.CALIBRATION_POINTS
        from road_address ra
        join lrm_position pos on ra.lrm_position_id = pos.id
        where floating = '0' and road_number = $roadNumber AND road_part_number = $roadPartNumber
        ORDER BY road_number, road_part_number, track_code, start_addr_m
      """
    val tuples = Q.queryNA[(Long, Long, Long, Int, Int, Long, Long, Long, Double, Double, Int,
      String, String, String, String, Int)](query).list
    tuples.map{
      case (id, roadNumber, roadPartNumber, track, discontinuity, startAddrMValue, endAddrMValue,
      linkId, startMValue, endMValue, sideCode, startDate, endDate, createdBy, createdDate, calibrationCode) =>
        RoadAddress(id, roadNumber, roadPartNumber, Track.apply(track), Discontinuity.apply(discontinuity), startAddrMValue, endAddrMValue, dateTimeParse(startDate), optDateTimeParse(endDate), linkId, startMValue, endMValue, calibrations(CalibrationCode.apply(calibrationCode), linkId, startMValue, endMValue, startAddrMValue, endAddrMValue, SideCode.apply(sideCode)))
    }
  }

  def fetchNextRoadNumber(current: Int) = {
    val query =
      s"""
          SELECT * FROM (
            SELECT ra.road_number
            FROM road_address ra
            WHERE floating = '0' and road_number > $current AND (end_date < sysdate OR end_date IS NULL)
            ORDER BY road_number ASC
          ) WHERE ROWNUM < 2
      """
    Q.queryNA[Int](query).firstOption
  }

  def fetchNextRoadPartNumber(roadNumber: Int, current: Int) = {
    val query =
      s"""
          SELECT * FROM (
            SELECT ra.road_part_number
            FROM road_address ra
            WHERE floating = '0' and road_number = $roadNumber  AND road_part_number > $current AND (end_date < sysdate OR end_date IS NULL)
            ORDER BY road_part_number ASC
          ) WHERE ROWNUM < 2
      """
    Q.queryNA[Int](query).firstOption
  }

  def update(roadAddress: RoadAddress) = {
    sqlu"""UPDATE ROAD_ADDRESS
        SET road_number = ${roadAddress.roadNumber},
            road_part_number= ${roadAddress.roadPartNumber},
            track_code = ${roadAddress.track.value},
            discontinuity= ${roadAddress.discontinuity.value},
            START_ADDR_M= ${roadAddress.startAddrMValue},
            END_ADDR_M= ${roadAddress.endAddrMValue},
            start_date= ${roadAddress.startDate},
            end_date= ${roadAddress.endDate}
        WHERE id = ${roadAddress.id}""".execute
  }


  def createMissingRoadAddress (missingRoadAddress: MissingRoadAddress) = {
    sqlu"""
           insert into missing_road_address (link_id, start_addr_m, end_addr_m, road_number, road_part_number, start_m, end_m, anomaly_code)
           values (${missingRoadAddress.linkId}, ${missingRoadAddress.startAddrMValue}, ${missingRoadAddress.endAddrMValue},
           ${missingRoadAddress.roadNumber}, ${missingRoadAddress.roadPartNumber},
           ${missingRoadAddress.startMValue}, ${missingRoadAddress.endMValue}, ${missingRoadAddress.anomaly.value})
           """.execute
  }

  def createMissingRoadAddress (linkId: Long, start_addr_m: Long, end_addr_m: Long, anomaly_code: Int) = {
    sqlu"""
           insert into missing_road_address (link_id, start_addr_m, end_addr_m,anomaly_code)
           values ($linkId, $start_addr_m, $end_addr_m, $anomaly_code)
           """.execute
  }

  def getMissingRoadAddresses(linkIds: Set[Long]): List[MissingRoadAddress] = {
    if (linkIds.size > 500) {
      getMissingByLinkIdMassQuery(linkIds)
    } else {
      val where = linkIds.isEmpty match {
        case true => return List()
        case false => s""" where link_id in (${linkIds.mkString(",")})"""
      }
      val query =
        s"""SELECT link_id, start_addr_m, end_addr_m, road_number, road_part_number, start_m, end_m, anomaly_code
            FROM missing_road_address $where"""
      Q.queryNA[(Long, Option[Long], Option[Long], Option[Long], Option[Long], Option[Double], Option[Double], Int)](query).list.map {
        case (linkId, startAddrM, endAddrM, road, roadPart, startM, endM, anomaly) =>
          MissingRoadAddress(linkId, startAddrM, endAddrM, UnknownOwnerRoad ,road, roadPart, startM, endM, Anomaly.apply(anomaly))
      }
    }
  }

  def getMissingByLinkIdMassQuery(linkIds: Set[Long]): List[MissingRoadAddress] = {
    MassQuery.withIds(linkIds) {
      idTableName =>
        val query =
          s"""SELECT link_id, start_addr_m, end_addr_m, road_number, road_part_number, start_m, end_m, anomaly_code
            FROM missing_road_address mra join $idTableName i on i.id = mra.link_id"""
        Q.queryNA[(Long, Option[Long], Option[Long], Option[Long], Option[Long], Option[Double], Option[Double], Int)](query).list.map {
          case (linkId, startAddrM, endAddrM, road, roadPart, startM, endM, anomaly) =>
            MissingRoadAddress(linkId, startAddrM, endAddrM, UnknownOwnerRoad, road, roadPart, startM, endM, Anomaly.apply(anomaly))
        }
    }
  }

  def getLrmPositionByLinkId(linkId: Long) = {
    sql"""
       select lrm.id, lrm.start_measure, lrm.end_measure
       from lrm_position lrm, road_address rda
       where lrm.id = rda.lrm_position_id and lrm.link_id = $linkId""".as[(Long, Double, Double)].list
  }

  def getLrmPositionMeasures(linkId: Long) = {
    sql"""
       select lrm.id, lrm.start_measure, lrm.end_measure
       from lrm_position lrm, road_address rda
       where lrm.id = rda.lrm_position_id
       and (lrm.start_measure != rda.start_addr_m or lrm.end_measure != rda.end_addr_m) and lrm.link_id = $linkId""".as[(Long, Double, Double)].list
  }

  def getLrmPositionRoadParts(linkId: Long, roadPart: Long) = {
    sql"""
       select lrm.id, lrm.start_measure, lrm.end_measure
       from lrm_position lrm, road_address rda
       where lrm.id = rda.lrm_position_id
       and lrm.link_id = $linkId and rda.road_part_number!= $roadPart""".as[(Long, Double, Double)].list
  }

  def getAllRoadAddressesByRange(startId: Long, returnAmount: Long) = {
      val query =
        s"""
       Select * From (
       select ra.id, ra.road_number, ra.road_part_number, ra.track_code,
       ra.discontinuity, ra.start_addr_m, ra.end_addr_m, pos.link_id, pos.start_measure, pos.end_measure,
       pos.side_code,
       ra.start_date, ra.end_date, ra.created_by, ra.created_date, ra.CALIBRATION_POINTS
       from road_address ra inner join lrm_position pos on ra.lrm_position_id = pos.id
       Where ra.id > ${startId}
       Order By ra.Id asc ) Where rownum <= ${returnAmount}
      """
      val tuples = Q.queryNA[(Long, Long, Long, Int, Int, Long, Long, Long, Double, Double, Int,
        String, String, String, String, Int)](query).list
      tuples.map {
        case (id, roadNumber, roadPartNumber, track, discontinuity, startAddrMValue, endAddrMValue,
        linkId, startMValue, endMValue, sideCode, startDate, endDate, createdBy, createdDate, calibrationCode) =>
          RoadAddress(id, roadNumber, roadPartNumber, Track.apply(track), Discontinuity.apply(discontinuity), startAddrMValue, endAddrMValue, dateTimeParse(startDate), Some(dateTimeParse(endDate)), linkId, startMValue, endMValue, calibrations(CalibrationCode.apply(calibrationCode), linkId, startMValue, endMValue, startAddrMValue, endAddrMValue, SideCode.apply(sideCode)))
      }
    }


  /**
    * Used to return the total ammount of road addresses (a road address is the junction between road_address table and lrm_position)
    *
    * @return The ammount of road addresses
    */
  def getRoadAddressAmount = {
    sql"""
       select count(*)
              from road_address ra
              where floating='0'
                    AND (end_date < sysdate OR end_date IS NULL)
      """.as[Long].firstOption.get
  }

  /**
    * Marks the road address identified by the supplied Id as eiher floating or not
    *
    * @param isFloating '0' for not floating, '1' for floating
    * @param roadAddressId The Id of a road addresss
    */
  def changeRoadAddressFloating(isFloating: Int, roadAddressId: Long): Unit = {
    sqlu"""
           Update road_address ra Set ra.floating = $isFloating Where ra.id = $roadAddressId
      """.execute
  }

  def changeRoadAddressFloating(float: Boolean, roadAddressId: Long): Unit = {
    changeRoadAddressFloating(float match {
      case true => 1
      case _ => 0}, roadAddressId)
  }

  def getValidRoadNumbers = {
    sql"""
       select distinct road_number
              from road_address ra
              where ra.floating = '0' AND (end_date < sysdate OR end_date IS NULL)
              order by road_number
      """.as[Long].list
  }

  def getValidRoadParts(roadNumber: Long) = {
    sql"""
       select distinct road_part_number
              from road_address ra
              where road_number = $roadNumber AND ra.floating = '0' AND (end_date < sysdate OR end_date IS NULL)
      """.as[Long].list
  }


  implicit val getDiscontinuity = GetResult[Discontinuity]( r=> Discontinuity.apply(r.nextInt()))

  implicit val getTrack = GetResult[Track]( r=> Track.apply(r.nextInt()))

  implicit val getCalibrationCode = GetResult[CalibrationCode]( r=> CalibrationCode.apply(r.nextInt()))
}
