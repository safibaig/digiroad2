package fi.liikennevirasto.digiroad2.linearasset.oracle

import _root_.oracle.spatial.geometry.JGeometry
import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.asset.oracle.{OracleSpatialAssetDao, Queries}
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase._
import org.joda.time.DateTime
import scala.slick.jdbc.{StaticQuery => Q, PositionedResult, GetResult, PositionedParameters, SetParameter}
import Q.interpolation
import scala.slick.driver.JdbcDriver.backend.Database
import Database.dynamicSession
import Q.interpolation
import fi.liikennevirasto.digiroad2.asset.oracle.Queries._
import _root_.oracle.sql.STRUCT
import com.github.tototoshi.slick.MySQLJodaSupport._

object OracleLinearAssetDao {
  implicit object GetByteArray extends GetResult[Array[Byte]] {
    def apply(rs: PositionedResult) = rs.nextBytes()
  }

  implicit object SetStruct extends SetParameter[STRUCT] {
    def apply(v: STRUCT, pp: PositionedParameters) {
      pp.setObject(v, java.sql.Types.STRUCT)
    }
  }

  def transformLink(link: (Long, Long, Int, Int, Array[Byte])) = {
    val (id, roadLinkId, sideCode, limit, pos) = link
    val points = JGeometry.load(pos).getOrdinatesArray.grouped(2)
    (id, roadLinkId, sideCode, limit, points.map { pointArray =>
      (pointArray(0), pointArray(1))
    }.toSeq)
  }

  def getSpeedLimitLinksByBoundingBox(bounds: BoundingRectangle): Seq[(Long, Long, Int, Int, Seq[(Double, Double)])] = {
    val boundingBox = new JGeometry(bounds.leftBottom.x, bounds.leftBottom.y, bounds.rightTop.x, bounds.rightTop.y, 3067)
    val geometry = storeGeometry(boundingBox, dynamicSession.conn)
    val speedLimits = sql"""
      select a.id, rl.id, pos.side_code, e.name_fi as speed_limit, SDO_AGGR_CONCAT_LINES(to_2d(sdo_lrs.dynamic_segment(rl.geom, pos.start_measure, pos.end_measure)))
        from ASSET a
        join ASSET_LINK al on a.id = al.asset_id
        join LRM_POSITION pos on al.position_id = pos.id
        join ROAD_LINK rl on pos.road_link_id = rl.id
        join PROPERTY p on a.asset_type_id = p.asset_type_id and p.public_id = 'rajoitus'
        join SINGLE_CHOICE_VALUE s on s.asset_id = a.id and s.property_id = p.id
        join ENUMERATED_VALUE e on s.enumerated_value_id = e.id
        where a.asset_type_id = 20 and SDO_FILTER(rl.geom, $geometry) = 'TRUE'
        group by a.id, rl.id, pos.side_code, e.name_fi
        """.as[(Long, Long, Int, Int, Array[Byte])].list
    speedLimits.map(transformLink)
  }

  def getSpeedLimitLinksById(id: Long): Seq[(Long, Long, Int, Int, Seq[(Double, Double)])] = {
    val speedLimits = sql"""
      select a.id, rl.id, pos.side_code, e.name_fi as speed_limit, SDO_AGGR_CONCAT_LINES(to_2d(sdo_lrs.dynamic_segment(rl.geom, pos.start_measure, pos.end_measure)))
        from ASSET a
        join ASSET_LINK al on a.id = al.asset_id
        join LRM_POSITION pos on al.position_id = pos.id
        join ROAD_LINK rl on pos.road_link_id = rl.id
        join PROPERTY p on a.asset_type_id = p.asset_type_id and p.public_id = 'rajoitus'
        join SINGLE_CHOICE_VALUE s on s.asset_id = a.id and s.property_id = p.id
        join ENUMERATED_VALUE e on s.enumerated_value_id = e.id
        where a.asset_type_id = 20 and a.id = $id
        group by a.id, rl.id, pos.side_code, e.name_fi
        """.as[(Long, Long, Int, Int, Array[Byte])].list
    speedLimits.map(transformLink)
  }

  def getSpeedLimits(id: Long): Seq[(Long, Seq[(Double, Double)])] = {
    val speedLimits = sql"""
      select a.id, SDO_AGGR_CONCAT_LINES(to_2d(sdo_lrs.dynamic_segment(rl.geom, pos.start_measure, pos.end_measure)))
        from ASSET a
        join ASSET_LINK al on a.id = al.asset_id
        join LRM_POSITION pos on al.position_id = pos.id
        join ROAD_LINK rl on pos.road_link_id = rl.id
        where a.asset_type_id = 20 and a.id = $id
        group by a.id, rl.id
        """.as[(Long, Array[Byte])].list
    speedLimits.map { case (id, pos) =>
      val points = JGeometry.load(pos).getOrdinatesArray.grouped(2)
      (id, points.map { pointArray =>
        (pointArray(0), pointArray(1))
      }.toSeq)
    }
  }

  def getSpeedLimitDetails(id: Long): (Option[String], Option[DateTime], Option[String], Option[DateTime], Int, Seq[(Long, Long, Int, Int, Seq[(Double, Double)])]) = {
    val (modifiedBy, modifiedDate, createdBy, createdDate, name) = sql"""
      select a.modified_by, a.modified_date, a.created_by, a.created_date, e.name_fi
      from ASSET a
      join PROPERTY p on a.asset_type_id = p.asset_type_id and p.public_id = 'rajoitus'
      join SINGLE_CHOICE_VALUE s on s.asset_id = a.id and s.property_id = p.id
      join ENUMERATED_VALUE e on s.enumerated_value_id = e.id
      where a.id = $id
    """.as[(Option[String], Option[DateTime], Option[String], Option[DateTime], Int)].first
    val speedLimitLinks = getSpeedLimitLinksById(id)
    (modifiedBy, modifiedDate, createdBy, createdDate, name, speedLimitLinks)
  }

  def getSpeedLimitLinkStartAndEndMeasure(id: Long, roadLinkId: Long): (Double, Double) = {
    sql"""
      select lrm.START_MEASURE, lrm.END_MEASURE
        from asset a
        join asset_link al on a.ID = al.ASSET_ID
        join lrm_position lrm on lrm.id = al.POSITION_ID
        join road_link rl on rl.id = lrm.ROAD_LINK_ID
        where a.id = $id
    """.as[(Double, Double)].list.head
  }

  def createSpeedLimit(creator: String, roadLinkId: Long, startMeasure: Double, endMeasure: Double): Long = {
    val assetId = OracleSpatialAssetDao.nextPrimaryKeySeqValue
    val lrmPositionId = OracleSpatialAssetDao.nextLrmPositionPrimaryKeySeqValue
    sqlu"""
      insert into asset(id, asset_type_id, created_by)
      values ($assetId, 20, $creator)
    """.execute()
    sqlu"""
      insert into lrm_position(id, start_measure, end_measure, road_link_id)
      values ($lrmPositionId, $startMeasure, $endMeasure, $roadLinkId)
    """.execute()
    sqlu"""
      insert into asset_link(asset_id, position_id)
      values ($assetId, $lrmPositionId)
    """.execute()
    val propertyId = Q.query[String, Long](Queries.propertyIdByPublicId).firstOption("rajoitus").get
    Queries.insertSingleChoiceProperty(assetId, propertyId, 50).execute()
    assetId
  }

  def splitSpeedLimit(id: Long, roadLinkId: Long, splitMeasure: Double, username: String): Long = {
    Queries.updateAssetModified(id, username).execute()
    val (startMeasure, endMeasure) = getSpeedLimitLinkStartAndEndMeasure(id, roadLinkId)
    val firstSplitLength = splitMeasure - startMeasure
    val secondSplitLength = endMeasure - splitMeasure
    val (existingLinkMeasures, createdLinkMeasures) = firstSplitLength > secondSplitLength match {
      case true => ((startMeasure, splitMeasure), (splitMeasure, endMeasure))
      case false => ((splitMeasure, endMeasure), (startMeasure, splitMeasure))
    }
    val (existingLinkStartMeasure, existingLinkEndMeasure) = existingLinkMeasures

    sql"""
      update LRM_POSITION
      set
        start_measure = $existingLinkStartMeasure,
        end_measure = $existingLinkEndMeasure
      where id = (
        select lrm.id
          from asset a
          join asset_link al on a.ID = al.ASSET_ID
          join lrm_position lrm on lrm.id = al.POSITION_ID
          join road_link rl on rl.id = lrm.ROAD_LINK_ID
          where a.id = $id)
    """.asUpdate.execute()

    createSpeedLimit(username, roadLinkId, createdLinkMeasures._1, createdLinkMeasures._2)
  }
}
