package fi.liikennevirasto.viite.process

import fi.liikennevirasto.digiroad2.asset.SideCode
import fi.liikennevirasto.digiroad2.linearasset.RoadLink
import fi.liikennevirasto.digiroad2.{ChangeInfo, GeometryUtils, Point, VVHClient}
import fi.liikennevirasto.viite._
import fi.liikennevirasto.viite.dao.RoadAddress
import org.slf4j.LoggerFactory

case class RoadAddressInfoMapping(changeType:Long, sourceLinkId: Long, targetLinkId: Long, sourceStartM: Double, sourceEndM: Double,
                                  targetStartM: Double, targetEndM: Double, targetStartM2: Option[Double], targetEndM2: Option[Double], sourceGeom: Seq[Point], targetGeom: Seq[Point],
                                  vvhTimeStamp: Option[Long] = None) extends BaseRoadAddressMapping {
  override def toString: String = {
    s"$sourceLinkId -> $targetLinkId: $sourceStartM-$sourceEndM ->  $targetStartM-$targetEndM, $sourceGeom -> $targetGeom"
  }
  /**
    * Test if this mapping matches the road address: Road address is on source link and overlap match is at least 99,9%
    * (overlap amount is the overlapping length divided by the smallest length)
    */
  def matches(roadAddress: RoadAddress): Boolean = {
    sourceLinkId == roadAddress.linkId &&
      (vvhTimeStamp.isEmpty || roadAddress.adjustedTimestamp < vvhTimeStamp.get) &&
      GeometryUtils.overlapAmount((roadAddress.startMValue, roadAddress.endMValue), (sourceStartM, sourceEndM)) > 0.001
  }

  val sourceDelta = sourceEndM - sourceStartM
  val targetDelta = targetEndM - targetStartM
  val sourceLen: Double = Math.abs(sourceDelta)
  val targetLen: Double = Math.abs(targetDelta)
  val coefficient: Double = targetDelta/sourceDelta
  /**
    * interpolate location mValue on starting measure to target location
    * @param mValue Source M value to map
    */
  def interpolate(mValue: Double): Double = {
    if (DefloatMapper.withinTolerance(mValue, sourceStartM))
      targetStartM
    else if (DefloatMapper.withinTolerance(mValue, sourceEndM))
      targetEndM
    else {
      // Affine transformation: y = ax + b
      val a = coefficient
      val b = targetStartM - sourceStartM
      a * mValue + b
    }
  }
}

object RoadAddressChangeInfoMapper extends RoadAddressMapper {
  private val logger = LoggerFactory.getLogger(getClass)

  private def createAddressMap(sources: Seq[ChangeInfo]): Seq[RoadAddressInfoMapping] = {
    val pseudoGeom = Seq(Point(0.0, 0.0), Point(1.0, 0.0))
    sources.map(ci => {
      ci.changeType match {
        case 1 =>
          logger.debug("Change info> oldId: "+ci.oldId+" newId: "+ci.newId+" changeType: "+ci.changeType)
          Some(RoadAddressInfoMapping(ci.changeType, ci.oldId.get, ci.newId.get, ci.oldStartMeasure.get, ci.oldEndMeasure.get,
            ci.newStartMeasure.get, ci.newEndMeasure.get, Option.empty[Double], Option.empty[Double], pseudoGeom, pseudoGeom, Some(ci.vvhTimeStamp)))
        case 2 =>
          logger.debug("Change info> oldId: "+ci.oldId+" newId: "+ci.newId+" changeType: "+ci.changeType)
          Some(RoadAddressInfoMapping(ci.changeType, ci.oldId.get, ci.newId.get, ci.oldStartMeasure.get, ci.oldEndMeasure.get,
            ci.newStartMeasure.get, ci.newEndMeasure.get, Option.empty[Double], Option.empty[Double], pseudoGeom, pseudoGeom, Some(ci.vvhTimeStamp)))
        case 5 =>
          logger.debug("Change info> oldId: "+ci.oldId+" newId: "+ci.newId+" changeType: "+ci.changeType)
          Some(RoadAddressInfoMapping(ci.changeType, ci.oldId.get, ci.newId.get, ci.oldStartMeasure.get, ci.oldEndMeasure.get,
            ci.newStartMeasure.get, ci.newEndMeasure.get, Option.empty[Double], Option.empty[Double], pseudoGeom, pseudoGeom, Some(ci.vvhTimeStamp)))
        case 6 =>
          logger.debug("Change info> oldId: "+ci.oldId+" newId: "+ci.newId+" changeType: "+ci.changeType)
          Some(RoadAddressInfoMapping(ci.changeType, ci.oldId.get, ci.newId.get, ci.oldStartMeasure.get, ci.oldEndMeasure.get,
            ci.newStartMeasure.get, ci.newEndMeasure.get, Option.empty[Double], Option.empty[Double], pseudoGeom, pseudoGeom, Some(ci.vvhTimeStamp)))
        case _ => None
      }
    }).filter(c => c.isDefined).map(_.get)
  }

  private def applyChanges(changes: Seq[Seq[ChangeInfo]], roadAddresses: Map[Long, Seq[RoadAddress]]): Map[Long, Seq[RoadAddress]] = {
    if (changes.isEmpty)
      roadAddresses
    else {
      val mapping = createAddressMap(changes.head)
      val mapped = roadAddresses.mapValues(_.flatMap(ra =>
        if (mapping.exists(_.matches(ra))) {
          val changeVVHTimestamp = mapping.head.vvhTimeStamp.get
          mapInfoRoadAddresses(mapping)(ra).map(_.copy(adjustedTimestamp = changeVVHTimestamp))
        }
        else
          Seq(ra)))
      applyChanges(changes.tail, mapped.values.toSeq.flatten.groupBy(_.linkId))
    }
  }

  private def mapInfoRoadAddresses(roadAddressMapping: Seq[RoadAddressInfoMapping])(ra: RoadAddress): Seq[RoadAddress] = {
    def truncate(geometry: Seq[Point], d1: Double, d2: Double) = {
      val startM = Math.max(Math.min(d1, d2), 0.0)
      val endM = Math.min(Math.max(d1, d2), GeometryUtils.geometryLength(geometry))
      GeometryUtils.truncateGeometry3D(geometry,
        startM, endM)
    }

    // When mapping contains a larger span (sourceStart, sourceEnd) than the road address then split the mapping
    def adjust(mapping: RoadAddressInfoMapping, startM: Double, endM: Double) = {
      if (withinTolerance(mapping.sourceStartM, startM) && withinTolerance(mapping.sourceEndM, endM))
        mapping
      else {
        val (newStartM, newEndM) =
          (if (withinTolerance(startM, mapping.sourceStartM) || startM < mapping.sourceStartM) mapping.sourceStartM else startM,
            if (withinTolerance(endM, mapping.sourceEndM) || endM > mapping.sourceEndM) mapping.sourceEndM else endM)
        val (newTargetStartM, newTargetEndM) = (mapping.interpolate(newStartM), mapping.interpolate(newEndM))
        val geomStartM = Math.min(mapping.sourceStartM, mapping.sourceEndM)
        val geomTargetStartM = Math.min(mapping.interpolate(mapping.sourceStartM), mapping.interpolate(mapping.sourceEndM))
        mapping.copy(sourceStartM = newStartM, sourceEndM = newEndM, sourceGeom =
          truncate(mapping.sourceGeom, newStartM - geomStartM, newEndM - geomStartM),
          targetStartM = newTargetStartM, targetEndM = newTargetEndM, targetGeom =
            truncate(mapping.targetGeom, newTargetStartM - geomTargetStartM, newTargetEndM - geomTargetStartM))
      }
    }

    roadAddressMapping.filter(_.matches(ra)).map(m => adjust(m, ra.startMValue, ra.endMValue)).map(adjMap => {
      val (sideCode, mappedGeom, (mappedStartAddrM, mappedEndAddrM)) =
        if (isDirectionMatch(adjMap))
          (ra.sideCode, adjMap.targetGeom, splitRoadAddressValues(ra, adjMap))
        else {
          (switchSideCode(ra.sideCode), adjMap.targetGeom.reverse,
            splitRoadAddressValues(ra, adjMap))
        }
      val (startM, endM) = (Math.min(adjMap.targetEndM, adjMap.targetStartM), Math.max(adjMap.targetEndM, adjMap.targetStartM))

      val startCP = ra.startCalibrationPoint match {
        case None => None
        case Some(cp) => if (cp.addressMValue == mappedStartAddrM) Some(cp.copy(linkId = adjMap.targetLinkId,
          segmentMValue = if (sideCode == SideCode.AgainstDigitizing) Math.max(startM, endM) else 0.0)) else None
      }
      val endCP = ra.endCalibrationPoint match {
        case None => None
        case Some(cp) => if (cp.addressMValue == mappedEndAddrM) Some(cp.copy(linkId = adjMap.targetLinkId,
          segmentMValue = if (sideCode == SideCode.TowardsDigitizing) Math.max(startM, endM) else 0.0)) else None
      }
      ra.copy(id = NewRoadAddress, linkId = adjMap.targetLinkId, startAddrMValue = startCP.map(_.addressMValue).getOrElse(mappedStartAddrM),
        endAddrMValue = endCP.map(_.addressMValue).getOrElse(mappedEndAddrM), floating = false,
        sideCode = sideCode, startMValue = startM, endMValue = endM, geom = mappedGeom, calibrationPoints = (startCP, endCP),
        adjustedTimestamp = VVHClient.createVVHTimeStamp())
    })
  }

  private def splitRoadAddressValues(roadAddress: RoadAddress, mapping: BaseRoadAddressMapping): (Long, Long) = {
    if (withinTolerance(roadAddress.startMValue, mapping.sourceStartM) && withinTolerance(roadAddress.endMValue, mapping.sourceEndM))
      (roadAddress.startAddrMValue, roadAddress.endAddrMValue)
    else {
      val (startM, endM) = GeometryUtils.overlap((roadAddress.startMValue, roadAddress.endMValue),(mapping.sourceStartM, mapping.sourceEndM)).get
      roadAddress.addressBetween(startM, endM)
    }
  }

  def resolveChangesToMap(roadAddresses: Map[Long, Seq[RoadAddress]], changedRoadLinks: Seq[RoadLink], changes: Seq[ChangeInfo]): Map[Long, Seq[RoadAddress]] = {
    val sections = generateSections(roadAddresses, changes)
    val originalAddressSections = groupByRoadSection(sections, roadAddresses.values.toSeq.flatten)
    preTransferCheckBySection(originalAddressSections)
    val groupedChanges = changes.groupBy(_.vvhTimeStamp).values.toSeq
    val appliedChanges = applyChanges(groupedChanges.sortBy(_.head.vvhTimeStamp), roadAddresses)
    //TODO: i might have to rewrite a portion of the recalculate, in detail the private def segmentize(addresses: Seq[RoadAddress], processed: Seq[RoadAddress]): Seq[RoadAddress] method in order to skip the segmentizaion if calibration points do exist
    val recalculated = LinkRoadAddressCalculator.recalculate(appliedChanges.flatMap(_._2).filter(_.id == fi.liikennevirasto.viite.NewRoadAddress).toSeq)
    val grouped = groupByRoadSection(sections, appliedChanges.values.toSeq.flatten)
    val result = postTransferCheckBySection(grouped, originalAddressSections)
    result.values.toSeq.flatten.groupBy(_.linkId)
  }

  //TODO: find out how to actually generate the startAddrMValue and endAddrMValue for the division cases, it's clashing with RoadAddressMapper - line 103
  private def generateSections(roadAddresses: Map[Long, Seq[RoadAddress]], changes: Seq[ChangeInfo]): Seq[RoadAddressSection] ={
    val roadAddressSeq = roadAddresses.values.toSeq.flatten
    val sections = partition(roadAddressSeq)
    val filteredChanges = changes.filter(c => (c.changeType == 5 || c.changeType == 6) && (roadAddresses.values.toSeq.flatten.map(_.linkId).contains(c.oldId.get)))
    val sects = filteredChanges.flatMap(fi => {
      val ra = roadAddressSeq.find(_.linkId == fi.oldId.getOrElse(-1))
      partition(Seq(ra.get.copy(linkId = fi.oldId.get, startMValue = fi.newStartMeasure.get, endMValue = fi.newEndMeasure.get)))
    })
    sections ++ sects
  }

  private def groupByRoadSection(sections: Seq[RoadAddressSection],
                                 roadAddresses: Seq[RoadAddress]): Map[RoadAddressSection, Seq[RoadAddress]] = {
    sections.map(section => section -> roadAddresses.filter(section.includes)).toMap
  }

  // TODO: Don't try to apply changes to invalid sections
  private def preTransferCheckBySection(sections: Map[RoadAddressSection, Seq[RoadAddress]]) = {
    sections.values.map( seq =>
      try {
        preTransferChecks(seq)
        true
      } catch {
        case ex: InvalidAddressDataException =>
          logger.info(s"Section had invalid road data ${seq.head.roadNumber}/${seq.head.roadPartNumber}: ${ex.getMessage}")
          false
      })
  }

  private def postTransferCheckBySection(sections: Map[RoadAddressSection, Seq[RoadAddress]],
                                         original: Map[RoadAddressSection, Seq[RoadAddress]]): Map[RoadAddressSection, Seq[RoadAddress]] = {
    sections.map(s =>
      try {
        postTransferChecks(s)
        s
      } catch {
        case ex: InvalidAddressDataException =>
          logger.info(s"Invalid address data after transfer on ${s._1}, not applying changes")
          s._1 -> original(s._1)
      }
    )
  }

  def isInfoDirectionMatch(r: RoadAddressInfoMapping): Boolean = {
    ((r.sourceStartM - r.sourceEndM) * (r.targetStartM - r.targetEndM)) > 0
  }



}
