package fi.liikennevirasto.digiroad2.util


import java.util.Properties
import javax.naming.OperationNotSupportedException

import fi.liikennevirasto.digiroad2.asset.SideCode.{AgainstDigitizing, BothDirections, TowardsDigitizing}
import fi.liikennevirasto.digiroad2.asset.oracle.OracleAssetDao
import fi.liikennevirasto.digiroad2.{TierekisteriAssetDataClient, TierekisteriLightingAssetClient, TierekisteriRoadWidthAssetClient, _}
import fi.liikennevirasto.digiroad2.asset.{SideCode, _}
import fi.liikennevirasto.digiroad2.linearasset.oracle.OracleLinearAssetDao
import fi.liikennevirasto.digiroad2.masstransitstop.oracle.Queries
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.pointasset.oracle.OracleTrafficSignDao
import fi.liikennevirasto.digiroad2.roadaddress.oracle.{RoadAddressDAO, RoadAddress => ViiteRoadAddress}
import org.apache.http.impl.client.HttpClientBuilder
import org.joda.time.DateTime

case class AddressSection(roadNumber: Long, roadPartNumber: Long, track: Track, startAddressMValue: Long, endAddressMValue: Option[Long])

trait TierekisteriAssetImporterOperations {

  val eventbus = new DummyEventBus
  lazy val dr2properties: Properties = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/digiroad2.properties"))
    props
  }
  lazy val roadLinkService = new RoadLinkService(vvhClient, eventbus, new DummySerializer)
  lazy val vvhClient: VVHClient = { new VVHClient(getProperty("digiroad2.VVHRestApiEndPoint")) }

  lazy val assetDao: OracleAssetDao = new OracleAssetDao
  lazy val roadAddressDao : RoadAddressDAO = new RoadAddressDAO

  def typeId: Int

  def withDynSession[T](f: => T): T

  def withDynTransaction[T](f: => T): T

  val tierekisteriClient: TierekisteriClientType

  def assetName: String

  type TierekisteriClientType <: TierekisteriAssetDataClient
  type TierekisteriAssetData = tierekisteriClient.TierekisteriType

  protected def getProperty(name: String) = {
    val property = dr2properties.getProperty(name)
    if(property != null)
      property
    else
      throw new RuntimeException(s"cannot find property $name")
  }

  protected def createAsset(section: AddressSection, trAssetData: TierekisteriAssetData): Unit

  //TODO Add protected to this method
  def getRoadAddressSections(trAssetData: TierekisteriAssetData): Seq[(AddressSection, TierekisteriAssetData)] = {
    Seq((AddressSection(trAssetData.roadNumber, trAssetData.startRoadPartNumber, trAssetData.track, trAssetData.startAddressMValue,
      if (trAssetData.endRoadPartNumber == trAssetData.startRoadPartNumber)
        Some(trAssetData.endAddressMValue)
      else
        None), trAssetData)) ++ {
      if (trAssetData.startRoadPartNumber != trAssetData.endRoadPartNumber) {
        val roadPartNumberSortedList = List(trAssetData.startRoadPartNumber, trAssetData.endRoadPartNumber).sorted
        (roadPartNumberSortedList.head until roadPartNumberSortedList.last).tail.map(part =>
          (AddressSection(trAssetData.roadNumber, part, trAssetData.track, 0L, None), trAssetData)) ++
          Seq((AddressSection(trAssetData.roadNumber, trAssetData.endRoadPartNumber, trAssetData.track, 0L, Some(trAssetData.endAddressMValue)), trAssetData))
      } else
        Seq[(AddressSection, TierekisteriAssetData)]()
    }
  }

  protected def getAllMunicipalities(): Seq[Int] = {
    withDynSession {
      assetDao.getMunicipalities
    }
  }

  protected def getAllViiteRoadNumbers(): Seq[Long] = {
    println("\nFetch Road Numbers From Viite")
    val roadNumbers = withDynSession {
      roadAddressDao.getRoadNumbers()
    }

    println("Road Numbers Fetched:")
    println(roadNumbers.mkString("\n"))

    roadNumbers
  }

  protected def calculateStartLrmByAddress(startAddress:  ViiteRoadAddress, section: AddressSection): Option[Double] = {
    if (startAddress.startAddrMValue >= section.startAddressMValue)
      startAddress.sideCode match {
        case TowardsDigitizing => Some(startAddress.startMValue)
        case AgainstDigitizing => Some(startAddress.endMValue)
        case _ => None
      }
    else
      startAddress.addressMValueToLRM(section.startAddressMValue)

  }

  protected def calculateEndLrmByAddress(endAddress: ViiteRoadAddress, section: AddressSection) = {
    if (endAddress.endAddrMValue <= section.endAddressMValue.getOrElse(endAddress.endAddrMValue))
      endAddress.sideCode match {
        case TowardsDigitizing => Some(endAddress.endMValue)
        case AgainstDigitizing => Some(endAddress.startMValue)
        case _ => None
      }
    else
      endAddress.addressMValueToLRM(section.endAddressMValue.get)
  }

  protected def getAllViiteRoadAddress(section: AddressSection) = {
    val addresses = roadAddressDao.getRoadAddress(roadAddressDao.withRoadAddressSinglePart(section.roadNumber, section.roadPartNumber, section.track.value, section.startAddressMValue, section.endAddressMValue))
    val vvhRoadLinks = roadLinkService.fetchVVHRoadlinks(addresses.map(ra => ra.linkId).toSet).filter(_.administrativeClass == State)
    addresses.map(ra => (ra, vvhRoadLinks.find(_.linkId == ra.linkId))).filter(_._2.isDefined)
  }

  protected def getAllViiteRoadAddress(roadNumber: Long, tracks: Seq[Track]) = {
    val addresses = roadAddressDao.getRoadAddress(roadAddressDao.withRoadNumber(roadNumber, tracks.map(_.value).toSet))
    val roadAddressLinks = addresses.map(ra => ra.linkId).toSet
    val vvhRoadLinks = roadLinkService.fetchVVHRoadlinks(roadAddressLinks).filter(_.administrativeClass == State)
    addresses.map(ra => (ra, vvhRoadLinks.find(_.linkId == ra.linkId))).filter(_._2.isDefined)
  }

  protected def getAllViiteRoadAddress(roadNumber: Long, roadPart: Long, track: Track) = {
    val addresses = roadAddressDao.getRoadAddress(roadAddressDao.withRoadNumber(roadNumber, roadPart, track.value))
    val roadAddressLinks = addresses.map(ra => ra.linkId).toSet
    val vvhRoadLinks = roadLinkService.fetchVVHRoadlinks(roadAddressLinks).filter(_.administrativeClass == State)
    addresses.map(ra => (ra, vvhRoadLinks.find(_.linkId == ra.linkId))).filter(_._2.isDefined)
  }

  protected def getAllTierekisteriAddressSections(roadNumber: Long) = {
    println("\nFetch Tierekisteri " + assetName + " by Road Number " + roadNumber)
    val trAsset = tierekisteriClient.fetchActiveAssetData(roadNumber)

    trAsset.foreach { tr => println(s"TR: address ${tr.roadNumber}/${tr.startRoadPartNumber}-${tr.endRoadPartNumber}/${tr.track.value}/${tr.startAddressMValue}-${tr.endAddressMValue}") }
    trAsset.map(_.asInstanceOf[TierekisteriAssetData]).flatMap(getRoadAddressSections)
  }

  protected def getAllTierekisteriAssets(roadNumber: Long) = {
    println("\nFetch Tierekisteri " + assetName + " by Road Number " + roadNumber)
    val trAssets = tierekisteriClient.fetchActiveAssetData(roadNumber)

    trAssets.foreach { tr => println(s"TR: address ${tr.roadNumber}/${tr.startRoadPartNumber}-${tr.endRoadPartNumber}/${tr.track.value}/${tr.startAddressMValue}-${tr.endAddressMValue}") }
    trAssets.map(_.asInstanceOf[TierekisteriAssetData])
  }

  protected def getAllTierekisteriHistoryAddressSection(roadNumber: Long, lastExecution: DateTime) = {
    println("\nFetch " + assetName + " History by Road Number " + roadNumber)
    val trAsset = tierekisteriClient.fetchHistoryAssetData(roadNumber, Some(lastExecution))

    trAsset.foreach { tr => println(s"TR: address ${tr.roadNumber}/${tr.startRoadPartNumber}-${tr.endRoadPartNumber}/${tr.track.value}/${tr.startAddressMValue}-${tr.endAddressMValue}") }
    trAsset.map(_.asInstanceOf[TierekisteriAssetData]).flatMap(getRoadAddressSections)
  }

  protected def getAllTierekisteriAddressSections(roadNumber: Long, roadPart: Long) = {
    println("\nFetch Tierekisteri " + assetName + " by Road Number " + roadNumber)
    val trAsset = tierekisteriClient.fetchActiveAssetData(roadNumber, roadPart)

    trAsset.foreach { tr => println(s"TR: address ${tr.roadNumber}/${tr.startRoadPartNumber}-${tr.endRoadPartNumber}/${tr.track.value}/${tr.startAddressMValue}-${tr.endAddressMValue}") }
    trAsset.map(_.asInstanceOf[TierekisteriAssetData]).flatMap(getRoadAddressSections)
  }

  protected def expireAssets(linkIds: Seq[Long]): Unit = {
    assetDao.expireAssetByTypeAndLinkId(typeId, linkIds)
  }

  protected def expireAssets(municipality: Int, administrativeClass: Option[AdministrativeClass] = None): Unit = {
    println("\nStart assets expiration in municipality %d".format(municipality))
    val roadLinksWithStateFilter = administrativeClass match {
      case Some(state) => roadLinkService.getVVHRoadLinksF(municipality).filter(_.administrativeClass == state).map(_.linkId)
      case _ => roadLinkService.getVVHRoadLinksF(municipality).map(_.linkId)
    }

    expireAssets(roadLinksWithStateFilter);

    println("\nEnd assets expiration in municipality %d".format(municipality))
  }

  def importAssets(): Unit = {
    //Expire all asset in state roads in all the municipalities
    val municipalities = getAllMunicipalities()
    /*municipalities.foreach { municipality =>
      withDynTransaction{
        expireAssets(municipality, Some(State))
      }
    }*/

    val roadNumbers = Seq(484)//getAllViiteRoadNumbers()

    roadNumbers.foreach {
      roadNumber =>
        //Fetch asset from Tierekisteri and then generates the sections foreach returned asset
        //For example if Tierekisteri returns
        //One asset with start part = 2, end part = 5, start address = 10, end address 20
        //We will generate the middle parts and return a AddressSection for each one
        val trAddressSections = getAllTierekisteriAddressSections(roadNumber)

        //For each section creates a new OTH asset
        trAddressSections.foreach {
          case (section, trAssetData) =>
            withDynTransaction {
              createAsset(section, trAssetData)
            }
        }
    }
  }

  def updateAssets(lastExecution: DateTime): Unit = {
    val roadNumbers = getAllViiteRoadNumbers()

    roadNumbers.foreach {
      roadNumber =>
        //Fetch asset changes from Tierekisteri and then generates the sections foreach returned asset change
        //For example if Tierekisteri returns
        //One asset with start part = 2, end part = 5, start address = 10, end address 20
        //We will generate the middle parts and return a AddressSection for each one
        val trHistoryAddressSections = getAllTierekisteriHistoryAddressSection(roadNumber, lastExecution)

        withDynTransaction {
          //Expire all the sections that have changes in tierekisteri
          val expiredSections = trHistoryAddressSections.foldLeft(Seq.empty[Long]) {
            case (sections, (section, trAssetData)) =>
              //If the road part number was already process we ignore it
              if(sections.contains(section.roadPartNumber)) {
                sections
              } else {
                //Get all existing road address in viite and expire all the assets on top of this roads
                val roadAddressLink = getAllViiteRoadAddress(section.roadNumber, section.roadPartNumber, section.track)
                expireAssets(roadAddressLink.map(_._1.linkId))
                sections ++ Seq(section.roadPartNumber)
              }
          }

          //Creates the assets on top of the expired sections
          expiredSections.foreach{
            roadPart =>
              //Fetch asset from Tierekisteri and then generates the sections foreach returned asset
              val trAddressSections = getAllTierekisteriAddressSections(roadNumber, roadPart)
              trAddressSections.foreach {
                case (section, trAssetData) =>
                createAsset(section, trAssetData)
              }
          }
        }
    }
  }
}

trait LinearAssetTierekisteriImporterOperations extends TierekisteriAssetImporterOperations{

  lazy val linearAssetService: LinearAssetService = new LinearAssetService(roadLinkService, eventbus)

  protected def calculateMeasures(roadAddress: ViiteRoadAddress, section: AddressSection): Option[Measures] = {
    val startAddrMValueCandidate = calculateStartLrmByAddress(roadAddress, section)
    val endAddrMValueCandidate = calculateEndLrmByAddress(roadAddress, section)

    (startAddrMValueCandidate, endAddrMValueCandidate) match {
      case (Some(startAddrMValue), Some(endAddrMValue)) if(startAddrMValue <= endAddrMValue) => Some(Measures(startAddrMValue, endAddrMValue))
      case (Some(startAddrMValue), Some(endAddrMValue)) => Some(Measures(endAddrMValue, startAddrMValue))
      case _ => None
    }
  }

  protected def createLinearAsset(vvhRoadlink: VVHRoadlink, roadAddress: ViiteRoadAddress, section: AddressSection, measures: Measures, trAssetData: TierekisteriAssetData)

  protected override def createAsset(section: AddressSection, trAssetData: TierekisteriAssetData): Unit = {
    println(s"Fetch Road Addresses from Viite: R:${section.roadNumber} P:${section.roadPartNumber} T:${section.track.value} ADDRM:${section.startAddressMValue}-${section.endAddressMValue.map(_.toString).getOrElse("")}")

    //Returns all the match Viite road address for the given section
    val roadAddressLink = getAllViiteRoadAddress(section)

    roadAddressLink
      .foreach { case (ra, roadlink) =>
        calculateMeasures(ra, section).map {
          measures =>
            createLinearAsset(roadlink.get, ra, section, measures, trAssetData)
        }
      }
  }
}

trait PointAssetTierekisteriImporterOperations extends TierekisteriAssetImporterOperations{

  protected def createPointAsset(roadAddress: ViiteRoadAddress, vvhRoadlink: VVHRoadlink, mValue: Double, trAssetData: TierekisteriAssetData)

  protected override def createAsset(section: AddressSection, trAssetData: TierekisteriAssetData): Unit = {
    println(s"Fetch Road Addresses from Viite: R:${section.roadNumber} P:${section.roadPartNumber} T:${section.track.value} ADDRM:${section.startAddressMValue}-${section.endAddressMValue.map(_.toString).getOrElse("")}")

    //Returns all the match Viite road address for the given section
    val roadAddressLink = getAllViiteRoadAddress(section)

    roadAddressLink
      .foreach { case (ra, roadlink) =>
        ra.addressMValueToLRM(section.startAddressMValue).map{
          mValue =>
            createPointAsset(ra, roadlink.get, mValue, trAssetData)
        }
      }
  }
}

class SpeedLimitsTierekisteriImporter extends LinearAssetTierekisteriImporterOperations {
  override def typeId: Int = 310
  override def assetName: String = "SpeedLimitState"
  override type TierekisteriClientType = TierekisteriTrafficSignAssetClient
  override def withDynSession[T](f: => T): T = OracleDatabase.withDynSession(f)
  override def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)
  override val tierekisteriClient = new TierekisteriTrafficSignAssetClient(getProperty("digiroad2.tierekisteriRestApiEndPoint"),
    getProperty("digiroad2.tierekisteri.enabled").toBoolean,
    HttpClientBuilder.create().build())

  override def updateAssets(lastExecution: DateTime): Unit = {
    throw new OperationNotSupportedException("UpdateAssets feature is not support on speed limits")
  }

  private val speedlimitDao: OracleLinearAssetDao = new OracleLinearAssetDao(vvhClient, roadLinkService)

  private val urbanAreaSpeedLimit = 50
  private val defaultSpeedLimit = 80
  private val startSpeedLimitSigns = Set(TrafficSignType.SpeedLimit, TrafficSignType.SpeedLimitZone, TrafficSignType.UrbanArea)
  private val endSpeedLimitSigns = Set(TrafficSignType.EndSpeedLimit, TrafficSignType.EndSpeedLimitZone, TrafficSignType.EndUrbanArea)

  private def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  private def getSpeedLimitValue(trAsset: TierekisteriAssetData) = {
    trAsset.assetType.trafficSignType match {
      case TrafficSignType.SpeedLimit  =>
        toInt(trAsset.assetValue)
      case TrafficSignType.EndSpeedLimit =>
        Some(defaultSpeedLimit)
      case TrafficSignType.SpeedLimitZone  =>
        toInt(trAsset.assetValue)
      case TrafficSignType.EndSpeedLimitZone =>
        Some(defaultSpeedLimit)
      case TrafficSignType.UrbanArea =>
        Some(urbanAreaSpeedLimit)
      case TrafficSignType.EndUrbanArea =>
        Some(defaultSpeedLimit)
      case _ =>
        None
    }
  }

  private def filterSectionTrafficSigns(trafficSigns: Seq[TierekisteriAssetData], roadAddress: ViiteRoadAddress, roadSide: RoadSide): Seq[TierekisteriAssetData] ={
    trafficSigns.filter(trSign => trSign.assetType.trafficSignType.group == TrafficSignTypeGroup.SpeedLimits &&
      trSign.endRoadPartNumber == roadAddress.roadPartNumber && trSign.startAddressMValue >= roadAddress.startAddrMValue &&
      trSign.startAddressMValue <= roadAddress.endAddrMValue && roadSide == trSign.roadSide)
  }

  private def splitRoadAddressSectionBySigns(trAssets: Seq[TierekisteriAssetData], ra: ViiteRoadAddress, roadSide: RoadSide): Seq[(AddressSection, Option[TierekisteriAssetData])] = {
    val sectionAssets = filterSectionTrafficSigns(trAssets, ra, roadSide)
    if(sectionAssets.isEmpty) {
      Seq((AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, ra.startAddrMValue, Some(ra.endAddrMValue)), None))
    }
    else{
      roadSide match {
        case RoadSide.Right =>
          val sortedAssets = sectionAssets.sortBy(_.startAddressMValue)
          val first = Seq((AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, ra.startAddrMValue, Some(sortedAssets.head.startAddressMValue)), None))
          val last = Seq((AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, sortedAssets.last.startAddressMValue, Some(ra.endAddrMValue)), Some(sortedAssets.last)))
          val intermediate = sortedAssets.zip(sortedAssets.tail).map{
            case (firstAsset, lastAsset) =>
              (AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, firstAsset.startAddressMValue, Some(lastAsset.startAddressMValue)), Some(firstAsset))
          }
          first ++ intermediate ++ last
        case _ =>
          val sortedAssets = sectionAssets.sortBy(- _.startAddressMValue)
          val first = Seq((AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, sortedAssets.head.startAddressMValue, Some(ra.endAddrMValue)), None))
          val last = Seq((AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, ra.startAddrMValue, Some(sortedAssets.last.startAddressMValue)), Some(sortedAssets.last)))
          val intermediate = sortedAssets.zip(sortedAssets.tail).map{
            case (firstAsset, lastAsset) =>
              (AddressSection(ra.roadNumber, ra.roadPartNumber, ra.track, lastAsset.startAddressMValue, Some(firstAsset.startAddressMValue)), Some(firstAsset))
          }
          first ++ intermediate ++ last
      }
    }
  }

  private def createSpeedLimit(roadAddress: ViiteRoadAddress, addressSection: AddressSection, trAssetOption: Option[TierekisteriAssetData], roadLinkOption: Option[VVHRoadlink]): Unit ={
    roadLinkOption.map{
      roadLink =>
        calculateMeasures(roadAddress, addressSection).map {
          measures =>
            trAssetOption.map {
              trAsset  =>
                createLinearAsset(roadLink, roadAddress, addressSection, measures, trAsset)
            }
        }
    }
  }

  private def getSideCode(raSideCode: SideCode, roadSide: RoadSide): SideCode = {
    roadSide match {
      case RoadSide.Right => raSideCode
      case RoadSide.Left => raSideCode match {
        case TowardsDigitizing => SideCode.AgainstDigitizing
        case AgainstDigitizing => SideCode.TowardsDigitizing
        case _ => SideCode.BothDirections
      }
      case _ => SideCode.BothDirections
    }
  }

  private def generateOneSideSpeedLimits(roadNumber: Long, roadSide: RoadSide, trAssets : Seq[TierekisteriAssetData]): Unit = {
    def getViiteRoadAddress(roadSide: RoadSide) = {
      roadSide match {
        case RoadSide.Left =>
          getAllViiteRoadAddress(roadNumber, Seq(Track.LeftSide, Track.Combined)).sortBy(r => (-r._1.roadPartNumber, -r._1.startAddrMValue))
        case _ =>
          getAllViiteRoadAddress(roadNumber, Seq(Track.RightSide, Track.Combined)).sortBy(r => (r._1.roadPartNumber, r._1.startAddrMValue))
      }
    }

    getViiteRoadAddress(roadSide).foldLeft[Option[tierekisteriClient.TierekisteriType]](None){
      case (trAsset, (roadAddress: ViiteRoadAddress, roadLink: Option[VVHRoadlink])) =>
        splitRoadAddressSectionBySigns(trAssets, roadAddress, roadSide).foldLeft(trAsset){
          case (previousTrAsset, (addressSection: AddressSection, beginTrAsset)) =>
            val currentTrAssetSign = beginTrAsset.orElse(previousTrAsset)
            createSpeedLimit(roadAddress, addressSection, currentTrAssetSign, roadLink)
            currentTrAssetSign
        }
    }
  }

  override def importAssets(): Unit = {
    //Expire all asset in state roads in all the municipalities
    val municipalities = getAllMunicipalities()
    municipalities.foreach { municipality =>
      withDynTransaction{
        expireAssets(municipality, Some(State))
      }
    }

    val roadNumbers = getAllViiteRoadNumbers()

    roadNumbers.foreach {
      roadNumber =>
        withDynTransaction{
          val trAssets = getAllTierekisteriAssets(roadNumber)

          //Generate all speed limits of the right side of the road
          generateOneSideSpeedLimits(roadNumber, RoadSide.Right, trAssets)

          //Generate all speed limits of the left side of the road
          generateOneSideSpeedLimits(roadNumber, RoadSide.Left, trAssets)
        }
    }
  }

  override protected def createLinearAsset(roadLink: VVHRoadlink, roadAddress: ViiteRoadAddress, section: AddressSection, measures: Measures, trAssetData: TierekisteriAssetData) = {
    val speedLimit = getSpeedLimitValue(trAssetData)
    if (measures.startMeasure != measures.endMeasure) {
      val assetId = linearAssetService.dao.createLinearAsset(typeId, roadLink.linkId, false, getSideCode(roadAddress.sideCode, trAssetData.roadSide).value,
        measures, "batch_process_speedlimit", vvhClient.roadLinkData.createVVHTimeStamp(), Some(roadLink.linkSource.value))

      linearAssetService.dao.insertValue(assetId, LinearAssetTypes.numericValuePropertyId, speedLimit.getOrElse(-1))
      println(s"Created OTH Speed Limit assets for road ${roadLink.linkId} from TR data with assetId $assetId")
    }
  }
}

class TrafficSignTierekisteriImporter extends PointAssetTierekisteriImporterOperations {
  override def typeId: Int = 300
  override def assetName = "trafficSigns"
  override type TierekisteriClientType = TierekisteriTrafficSignAssetClient
  override def withDynSession[T](f: => T): T = OracleDatabase.withDynSession(f)
  override def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)
  override val tierekisteriClient = new TierekisteriTrafficSignAssetClient(getProperty("digiroad2.tierekisteriRestApiEndPoint"),
    getProperty("digiroad2.tierekisteri.enabled").toBoolean,
    HttpClientBuilder.create().build())

  private val typePublicId = "trafficSigns_type"
  private val valuePublicId = "trafficSigns_value"
  private val infoPublicId = "trafficSigns_info"

  private val additionalInfoTypeGroups = Set(TrafficSignTypeGroup.GeneralWarningSigns, TrafficSignTypeGroup.TurningRestrictions)
  private val supportedTrafficSigns = Set[TRTrafficSignType](TRTrafficSignType.SpeedLimit, TRTrafficSignType.EndSpeedLimit, TRTrafficSignType.SpeedLimitZone, TRTrafficSignType.EndSpeedLimitZone,
    TRTrafficSignType.UrbanArea, TRTrafficSignType.EndUrbanArea, TRTrafficSignType.PedestrianCrossing, TRTrafficSignType.MaximumLength, TRTrafficSignType.Warning,
    TRTrafficSignType.NoLeftTurn, TRTrafficSignType.NoRightTurn, TRTrafficSignType.NoUTurn)

  private def generateProperties(trAssetData: TierekisteriAssetData) = {
    val trafficType = trAssetData.assetType.trafficSignType
    val typeProperty = SimpleProperty(typePublicId, Seq(PropertyValue(trafficType.value.toString)))
    val valueProperty = additionalInfoTypeGroups.exists(group => group == trafficType.group) match {
      case true => SimpleProperty(infoPublicId, Seq(PropertyValue(trAssetData.assetValue)))
      case _ => SimpleProperty(valuePublicId, Seq(PropertyValue(trAssetData.assetValue)))
    }

    Set(typeProperty, valueProperty)
  }

  private def getSideCode(raSideCode: SideCode, roadSide: RoadSide): SideCode = {
    roadSide match {
      case RoadSide.Right => raSideCode
      case RoadSide.Left => raSideCode match {
        case TowardsDigitizing => SideCode.AgainstDigitizing
        case AgainstDigitizing => SideCode.TowardsDigitizing
        case _ => SideCode.BothDirections
      }
      case _ => SideCode.BothDirections
    }
  }

  override protected def createPointAsset(roadAddress: ViiteRoadAddress, vvhRoadlink: VVHRoadlink, mValue: Double, trAssetData: TierekisteriAssetData): Unit = {
    if(supportedTrafficSigns.contains(trAssetData.assetType))
      GeometryUtils.calculatePointFromLinearReference(vvhRoadlink.geometry, mValue).map{
        point =>
          val trafficSign = IncomingTrafficSign(point.x, point.y, vvhRoadlink.linkId, generateProperties(trAssetData),
            getSideCode(roadAddress.sideCode, trAssetData.roadSide).value, Some(GeometryUtils.calculateBearing(vvhRoadlink.geometry)))
          OracleTrafficSignDao.create(trafficSign, mValue, "batch_process_trafficSigns", vvhRoadlink.municipalityCode,
            VVHClient.createVVHTimeStamp(), vvhRoadlink.linkSource)
      }
  }
}

class LitRoadTierekisteriImporter extends LinearAssetTierekisteriImporterOperations {

  override def typeId: Int = 100
  override def assetName = "lighting"
  override type TierekisteriClientType = TierekisteriLightingAssetClient
  override def withDynSession[T](f: => T): T = OracleDatabase.withDynSession(f)
  override def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)
  override val tierekisteriClient = new TierekisteriLightingAssetClient(getProperty("digiroad2.tierekisteriRestApiEndPoint"),
    getProperty("digiroad2.tierekisteri.enabled").toBoolean,
    HttpClientBuilder.create().build())

  override protected def createLinearAsset(vvhRoadlink: VVHRoadlink, roadAddress: ViiteRoadAddress, section: AddressSection, measures: Measures, trAssetData: TierekisteriAssetData): Unit = {
    if (measures.startMeasure != measures.endMeasure) {
      val assetId = linearAssetService.dao.createLinearAsset(typeId, vvhRoadlink.linkId, false, SideCode.BothDirections.value,
        measures, "batch_process_" + assetName, vvhClient.roadLinkData.createVVHTimeStamp(), Some(vvhRoadlink.linkSource.value))

      linearAssetService.dao.insertValue(assetId, LinearAssetTypes.numericValuePropertyId, 1)
      println(s"Created OTH $assetName assets for ${vvhRoadlink.linkId} from TR data with assetId $assetId")
    }
  }
}

class RoadWidthTierekisteriImporter extends LinearAssetTierekisteriImporterOperations {

  override def typeId: Int = 100
  override def assetName = "roadWidth"
  override type TierekisteriClientType = TierekisteriRoadWidthAssetClient
  override def withDynSession[T](f: => T): T = OracleDatabase.withDynSession(f)
  override def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)

  override val tierekisteriClient = new TierekisteriRoadWidthAssetClient(getProperty("digiroad2.tierekisteriRestApiEndPoint"),
    getProperty("digiroad2.tierekisteri.enabled").toBoolean,
    HttpClientBuilder.create().build())

  override protected def createLinearAsset(vvhRoadlink: VVHRoadlink, roadAddress: ViiteRoadAddress, section: AddressSection, measures: Measures, trAssetData: TierekisteriAssetData): Unit = {
    if (measures.startMeasure != measures.endMeasure) {
      val assetId = linearAssetService.dao.createLinearAsset(typeId, vvhRoadlink.linkId, false, SideCode.BothDirections.value,
        measures, "batch_process_" + assetName, vvhClient.roadLinkData.createVVHTimeStamp(), Some(vvhRoadlink.linkSource.value))

      linearAssetService.dao.insertValue(assetId, LinearAssetTypes.numericValuePropertyId, trAssetData.assetValue)
      println(s"Created OTH " + assetName + " assets for $linkId from TR data with assetId $assetId")
    }
  }

}