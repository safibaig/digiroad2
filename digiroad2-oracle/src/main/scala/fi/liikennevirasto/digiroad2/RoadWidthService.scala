package fi.liikennevirasto.digiroad2

import fi.liikennevirasto.digiroad2.asset.{Municipality, Private, SideCode, UnknownLinkType}
import fi.liikennevirasto.digiroad2.linearasset._
import fi.liikennevirasto.digiroad2.linearasset.oracle.OracleLinearAssetDao
import fi.liikennevirasto.digiroad2.util.{LinearAssetUtils, PolygonTools}
import org.joda.time.DateTime


class RoadWidthService(roadLinkServiceImpl: RoadLinkService, eventBusImpl: DigiroadEventBus) extends LinearAssetOperations {
  override def roadLinkService: RoadLinkService = roadLinkServiceImpl
  override def dao: OracleLinearAssetDao = new OracleLinearAssetDao(roadLinkServiceImpl.vvhClient, roadLinkServiceImpl)
  override def eventBus: DigiroadEventBus = eventBusImpl
  override def vvhClient: VVHClient = roadLinkServiceImpl.vvhClient
  override def polygonTools: PolygonTools = new PolygonTools()

  override def getUncheckedLinearAssets(areas: Option[Set[Int]]) = throw new UnsupportedOperationException("Not supported method")

  val RoadWidthAssetTypeId: Int = 120

  override protected def getByRoadLinks(typeId: Int, roadLinks: Seq[RoadLink], changes: Seq[ChangeInfo]): Seq[PieceWiseLinearAsset] = {

    val linkIds = roadLinks.map(_.linkId)
    val removedLinkIds = LinearAssetUtils.deletedRoadLinkIds(changes, roadLinks)
    val existingAssets =
      withDynTransaction {
            dao.fetchLinearAssetsByLinkIds(RoadWidthAssetTypeId, linkIds ++ removedLinkIds, LinearAssetTypes.numericValuePropertyId).filterNot(_.expired)
        }

    val (assetsOnChangedLinks, assetsWithoutChangedLinks) = existingAssets.partition(a => LinearAssetUtils.newChangeInfoDetected(a, changes))

    val projectableTargetRoadLinks = roadLinks.filter(rl => rl.linkType.value == UnknownLinkType.value || rl.isCarTrafficRoad)

    val (expiredRoadWidthAssetIds, newAndUpdatedRoadWidthAssets) = getRoadWidthAssetChanges(existingAssets, roadLinks, changes)

    //combinedAssets são os assetsExistents , excluindo os que n tem changeLink o que sao para expirar e os que sao para atualizar para depois incluir todos
    val combinedAssets = assetsOnChangedLinks.filterNot(
      a => expiredRoadWidthAssetIds.contains(a.id) || newAndUpdatedRoadWidthAssets.exists(_.id == a.id) ) ++ newAndUpdatedRoadWidthAssets


    val filledNewAssets = fillNewRoadLinksWithPreviousAssetsData(projectableTargetRoadLinks,
      combinedAssets, assetsOnChangedLinks, changes) ++ assetsWithoutChangedLinks

    val newAssets = newAndUpdatedRoadWidthAssets.filterNot(a => filledNewAssets.exists(f => f.linkId == a.linkId)) ++ filledNewAssets

    val timing = System.currentTimeMillis
    if (newAssets.nonEmpty) {
      logger.info("Transferred %d assets in %d ms ".format(newAssets.length, System.currentTimeMillis - timing))
    }
    val groupedAssets = (existingAssets.filterNot(a => expiredRoadWidthAssetIds.contains(a.id) || newAssets.exists(_.linkId == a.linkId)) ++ newAssets).groupBy(_.linkId)
    val (filledTopology, changeSet) = NumericalLimitFiller.fillTopology(roadLinks, groupedAssets, typeId)

    val expiredAssetIds = existingAssets.filter(asset => removedLinkIds.contains(asset.linkId)).map(_.id).toSet ++
      changeSet.expiredAssetIds ++ expiredRoadWidthAssetIds

/*    val mValueAdjustments = newAndUpdatedPavingAssets.filter(_.id != 0).map( a =>
      MValueAdjustment(a.id, a.linkId, a.startMeasure, a.endMeasure)
    )*/
    eventBus.publish("linearAssets:update", changeSet.copy(expiredAssetIds = expiredAssetIds.filterNot(_ == 0L)
      /*,adjustedMValues = changeSet.adjustedMValues ++ mValueAdjustments*/))

    //Remove the asset ids ajusted in the "linearAssets:update" otherwise if the "linearAssets:saveProjectedLinearAssets" is executed after the "linearAssets:update"
    //it will update the mValues to the previous ones
    eventBus.publish("linearAssets:saveProjectedLinearAssets", newAssets.filterNot(a => changeSet.adjustedMValues.exists(_.assetId == a.id)))

    filledTopology
  }

  def getRoadWidthAssetChanges(existingLinearAssets: Seq[PersistedLinearAsset], roadLinks: Seq[RoadLink],
                            changeInfos: Seq[ChangeInfo]): (Set[Long], Seq[PersistedLinearAsset]) = {
//    The goal of this user story is to add a new road width asset with default value, when a new road link is added to VVH.
//    The functionality in this user story should include only municipality and private road links.
//    Group last vvhchanges by link id

    def fillIncompletedRoadlinks(assets: Seq[PersistedLinearAsset], roadLink: RoadLink, changeInfo: ChangeInfo ): Seq[PersistedLinearAsset] = {
      val pointsOfInterest = (assets.map(_.startMeasure) ++ assets.map(_.endMeasure) ++  Seq(GeometryUtils.geometryLength(roadLink.geometry))).distinct.sorted

      if(pointsOfInterest.length < 2)
        return Seq()

      val pieces = pointsOfInterest.zip(pointsOfInterest.tail)
      pieces.flatMap { measures =>
        Some(PersistedLinearAsset(0L, roadLink.linkId, SideCode.BothDirections.value, Some(NumericValue(MTKClassWidth.apply(roadLink.MTKClass).width)),
          measures._1, measures._2, None, None, None, None, false, RoadWidthAssetTypeId, changeInfo.vvhTimeStamp, None, linkSource = roadLink.linkSource))
      }
    }

    val roadLinkAdminClass = roadLinks.filter(road => road.administrativeClass == Municipality || road.administrativeClass == Private)
    val roadWithMTKClass = roadLinkAdminClass.filter(road => MTKClassWidth.values.toSeq.contains(MTKClassWidth.apply(road.MTKClass)))
    //por agora fica que n pode existir asset, preciso de confirmar quando o asset é apenas uma parte do link
    //val (roadWithAsset, roadWithoutAsset) = roadWithMTKClass.partition(_.linkId == existingLinearAssets.map(_.linkId))

    val lastChanges = changeInfos.filter(_.newId.isDefined).groupBy(_.newId.get).mapValues(c => c.maxBy(_.vvhTimeStamp))

    //Map all existing assets by roadlink and changeinfo
    val changedAssets = lastChanges.map{
      case (linkId, changeInfo) =>
        (roadWithMTKClass.find(road => road.linkId == linkId ), changeInfo, existingLinearAssets.filter(_.linkId == linkId))
    }

//    val expiredAssetsIds = changedAssets.flatMap {
//      case (Some(roadlink), changeInfo, assets) =>
//        if (assets.nonEmpty)
//          assets.filter(asset => asset.vvhTimeStamp < changeInfo.vvhTimeStamp &&
//            math.abs((asset.endMeasure - asset.startMeasure) - GeometryUtils.geometryLength(roadlink.geometry)) > 0.001 ).map(_.id)
//        else
//          List()
//      case _ =>
//        List()
//    }.toSet[Long]

    val newAndUpdatedAssets = changedAssets.flatMap{
      //a direccao do asset criado n deveria ser igual ao road?
      case (Some(roadlink), changeInfo, assets) if assets.isEmpty =>
            Some(PersistedLinearAsset(0L, roadlink.linkId, SideCode.BothDirections.value, Some(NumericValue(MTKClassWidth.apply(roadlink.MTKClass).width)), 0,
              GeometryUtils.geometryLength(roadlink.geometry), None, None, None, None, false,
              RoadWidthAssetTypeId, changeInfo.vvhTimeStamp, None, linkSource = roadlink.linkSource))
      case (Some(roadlink), changeInfo, assets) =>
        fillIncompletedRoadlinks(assets, roadlink, changeInfo).filterNot(a => existingLinearAssets.exists(asset => a.linkId == asset.linkId && a.startMeasure == asset.startMeasure && a.endMeasure == asset.endMeasure))
      case _ =>
        None
    }.toSeq

    (Set(), newAndUpdatedAssets)
  }

  override def persistProjectedLinearAssets(newLinearAssets: Seq[PersistedLinearAsset]): Unit ={
    if (newLinearAssets.nonEmpty)
      logger.info("Saving projected linear assets")

    val (toInsert, toUpdate) = newLinearAssets.partition(_.id == 0L)
    withDynTransaction {
        val roadLinks = roadLinkService.getRoadLinksAndComplementariesFromVVH(newLinearAssets.map(_.linkId).toSet, newTransaction = false)
        val persisted = dao.fetchLinearAssetsByIds(toUpdate.map(_.id).toSet, LinearAssetTypes.numericValuePropertyId).groupBy(_.id)
      updateProjected(toUpdate, persisted)

      if (newLinearAssets.nonEmpty)
        logger.info("Updated ids/linkids " + toUpdate.map(a => (a.id, a.linkId)))

      toInsert.foreach{ linearAsset =>
        val id = dao.createLinearAsset(linearAsset.typeId, linearAsset.linkId, linearAsset.expired, linearAsset.sideCode,
          Measures(linearAsset.startMeasure, linearAsset.endMeasure), linearAsset.createdBy.getOrElse(LinearAssetTypes.VvhGenerated), linearAsset.vvhTimeStamp, getLinkSource(roadLinks.find(_.linkId == linearAsset.linkId)))
        linearAsset.value match {
          case Some(intValue) =>
            dao.insertValue(id, LinearAssetTypes.numericValuePropertyId, intValue.toString.toInt )
          case _ => None
        }
      }
      if (newLinearAssets.nonEmpty)
        logger.info("Added assets for linkids " + toInsert.map(_.linkId))
    }
  }

  override protected def updateProjected(toUpdate: Seq[PersistedLinearAsset], persisted: Map[Long, Seq[PersistedLinearAsset]]) = {
    def valueChanged(assetToPersist: PersistedLinearAsset, persistedLinearAsset: Option[PersistedLinearAsset]) = {
      !persistedLinearAsset.exists(_.value == assetToPersist.value)
    }
    def mValueChanged(assetToPersist: PersistedLinearAsset, persistedLinearAsset: Option[PersistedLinearAsset]) = {
      !persistedLinearAsset.exists(pl => pl.startMeasure == assetToPersist.startMeasure &&
        pl.endMeasure == assetToPersist.endMeasure &&
        pl.vvhTimeStamp == assetToPersist.vvhTimeStamp)
    }
    def sideCodeChanged(assetToPersist: PersistedLinearAsset, persistedLinearAsset: Option[PersistedLinearAsset]) = {
      !persistedLinearAsset.exists(_.sideCode == assetToPersist.sideCode)
    }
    toUpdate.foreach { linearAsset =>
      val persistedLinearAsset = persisted.getOrElse(linearAsset.id, Seq()).headOption
      val id = linearAsset.id
      if (valueChanged(linearAsset, persistedLinearAsset)) {
        linearAsset.value match {
          case Some(NumericValue(intValue)) =>
            dao.updateValue(id, intValue, LinearAssetTypes.numericValuePropertyId, LinearAssetTypes.VvhGenerated)
          case _ => None
        }
      }
      if (mValueChanged(linearAsset, persistedLinearAsset)) dao.updateMValues(linearAsset.id, (linearAsset.startMeasure, linearAsset.endMeasure), linearAsset.vvhTimeStamp)
      if (sideCodeChanged(linearAsset, persistedLinearAsset)) dao.updateSideCode(linearAsset.id, SideCode(linearAsset.sideCode))
    }
  }

  override protected def updateWithoutTransaction(ids: Seq[Long], value: Value, username: String, measures: Option[Measures] = None): Seq[Long] = {
    if (ids.isEmpty)
      return ids

    ids.flatMap { id =>
      updateValueByExpiration(id, value.asInstanceOf[NumericValue], LinearAssetTypes.numericValuePropertyId, username, measures)
    }
  }

  override protected def createWithoutTransaction(typeId: Int, linkId: Long, value: Value, sideCode: Int, measures: Measures, username: String, vvhTimeStamp: Long, roadLink: Option[RoadLinkLike], fromUpdate: Boolean = false,
                                                  createdByFromUpdate: Option[String] = Some(""),
                                                  createdDateTimeFromUpdate: Option[DateTime] = Some(DateTime.now())): Long = {
    val id = dao.createLinearAsset(typeId, linkId, expired = false, sideCode, measures, username,
      vvhTimeStamp, getLinkSource(roadLink), fromUpdate, createdByFromUpdate, createdDateTimeFromUpdate)
    value match {
      case NumericValue(intValue) =>
        dao.insertValue(id, LinearAssetTypes.numericValuePropertyId, intValue)
      case _ => None
    }
    id
  }

}
