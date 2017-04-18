package fi.liikennevirasto.digiroad2

import com.newrelic.api.agent.NewRelic
import fi.liikennevirasto.digiroad2.asset.Asset._
import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.authentication.{RequestHeaderAuthentication, UnauthenticatedException, UserNotFoundException}
import fi.liikennevirasto.digiroad2.linearasset._
import fi.liikennevirasto.digiroad2.pointasset.oracle.IncomingServicePoint
import fi.liikennevirasto.digiroad2.user.{User, UserProvider}
import fi.liikennevirasto.digiroad2.util.RoadAddressException
import fi.liikennevirasto.digiroad2.util.Track
import org.apache.http.HttpStatus
import fi.liikennevirasto.digiroad2.util.GMapUrlSigner
import org.apache.commons.lang3.StringUtils.isBlank
import org.apache.http.HttpStatus
import org.joda.time.DateTime
import org.json4s._
import org.scalatra._
import org.scalatra.json._
import org.slf4j.LoggerFactory

case class LinkProperties(linkId: Long, functionalClass: Int, linkType: LinkType, trafficDirection: TrafficDirection)

case class ExistingLinearAsset(id: Long, linkId: Long)

case class NewNumericValueAsset(linkId: Long, startMeasure: Double, endMeasure: Double, value: Int, sideCode: Int)
case class NewTextualValueAsset(linkId: Long, startMeasure: Double, endMeasure: Double, value: String, sideCode: Int)

case class NewProhibition(linkId: Long, startMeasure: Double, endMeasure: Double, value: Seq[ProhibitionValue], sideCode: Int)

case class NewMaintenanceRoad(linkId: Long, startMeasure: Double, endMeasure: Double, value: Seq[Properties], sideCode: Int)

class Digiroad2Api(val roadLinkService: RoadLinkService,
                   val speedLimitService: SpeedLimitService,
                   val obstacleService: ObstacleService = Digiroad2Context.obstacleService,
                   val railwayCrossingService: RailwayCrossingService = Digiroad2Context.railwayCrossingService,
                   val directionalTrafficSignService: DirectionalTrafficSignService = Digiroad2Context.directionalTrafficSignService,
                   val servicePointService: ServicePointService = Digiroad2Context.servicePointService,
                   val vvhClient: VVHClient,
                   val massTransitStopService: MassTransitStopService,
                   val linearAssetService: LinearAssetService,
                   val manoeuvreService: ManoeuvreService = Digiroad2Context.manoeuvreService,
                   val pedestrianCrossingService: PedestrianCrossingService = Digiroad2Context.pedestrianCrossingService,
                   val userProvider: UserProvider = Digiroad2Context.userProvider,
                   val assetPropertyService: AssetPropertyService = Digiroad2Context.assetPropertyService,
                   val trafficLightService: TrafficLightService = Digiroad2Context.trafficLightService)
  extends ScalatraServlet
    with JacksonJsonSupport
    with CorsSupport
    with RequestHeaderAuthentication
    with GZipSupport {
    val serviceRoadTypeid=290
    val logger = LoggerFactory.getLogger(getClass)
  // Somewhat arbitrarily chosen limit for bounding box (Math.abs(y1 - y2) * Math.abs(x1 - x2))
  val MAX_BOUNDING_BOX = 100000000

  case object DateTimeSerializer extends CustomSerializer[DateTime](format => ( {
    case _ => throw new NotImplementedError("DateTime deserialization")
  }, {
    case d: DateTime => JString(d.toString(DateTimePropertyFormat))
  }))

  case object SideCodeSerializer extends CustomSerializer[SideCode](format => ( {
    null
  }, {
    case s: SideCode => JInt(s.value)
  }))

  case object TrafficDirectionSerializer extends CustomSerializer[TrafficDirection](format => ( {
    case JString(direction) => TrafficDirection(direction)
  }, {
    case t: TrafficDirection => JString(t.toString)
  }))

  case object DayofWeekSerializer extends CustomSerializer[ValidityPeriodDayOfWeek](format => ( {
    case JString(dayOfWeek) => ValidityPeriodDayOfWeek(dayOfWeek)
  }, {
    case d: ValidityPeriodDayOfWeek => JString(d.toString)
  }))

  case object LinkTypeSerializer extends CustomSerializer[LinkType](format => ( {
    case JInt(linkType) => LinkType(linkType.toInt)
  }, {
    case lt: LinkType => JInt(BigInt(lt.value))
  }))

  protected implicit val jsonFormats: Formats = DefaultFormats + DateTimeSerializer + SideCodeSerializer + TrafficDirectionSerializer + LinkTypeSerializer + DayofWeekSerializer

  before() {
    contentType = formats("json") + "; charset=utf-8"
    try {
      authenticateForApi(request)(userProvider)
      if (request.isWrite && !userProvider.getCurrentUser().hasWriteAccess()) {
        halt(Unauthorized("No write permissions"))
      }
    } catch {
      case ise: IllegalStateException => halt(Unauthorized("Authentication error: " + ise.getMessage))
    }
    response.setHeader(Digiroad2Context.Digiroad2ServerOriginatedResponseHeader, "true")
  }

  case class StartupParameters(lon: Double, lat: Double, zoom: Int)

  get("/startupParameters") {
    val (east, north, zoom) = {
      val config = userProvider.getCurrentUser().configuration
      (config.east.map(_.toDouble), config.north.map(_.toDouble), config.zoom.map(_.toInt))
    }
    StartupParameters(east.getOrElse(390000), north.getOrElse(6900000), zoom.getOrElse(2))
  }
  get("/masstransitstopgapiurl"){
    val lat =params.get("latitude").getOrElse(halt(BadRequest("Bad coordinates")))
    val lon =params.get("longitude").getOrElse(halt(BadRequest("Bad coordinates")))
    val heading =params.get("heading").getOrElse(halt(BadRequest("Bad coordinates")))
    val oldapikeyurl=s"//maps.googleapis.com/maps/api/streetview?key=AIzaSyBh5EvtzXZ1vVLLyJ4kxKhVRhNAq-_eobY&size=360x180&location=$lat,$lon&fov=110&heading=$heading&pitch=-10&sensor=false'"
    try {
      val urlsigner = new GMapUrlSigner()
      Map("gmapiurl" -> urlsigner.signRequest(lat,lon,heading))
    } catch
      {
        case e: Exception => Map("gmapiurl" -> oldapikeyurl)
      }
  }

  get("/massTransitStops") {
    val user = userProvider.getCurrentUser()
    val bbox = params.get("bbox").map(constructBoundingRectangle).getOrElse(halt(BadRequest("Bounding box was missing")))
    validateBoundingBox(bbox)
    massTransitStopService.getByBoundingBox(user, bbox).map { stop =>
      Map("id" -> stop.id,
        "nationalId" -> stop.nationalId,
        "stopTypes" -> stop.stopTypes,
        "municipalityNumber" -> stop.municipalityCode,
        "lat" -> stop.lat,
        "lon" -> stop.lon,
        "validityDirection" -> stop.validityDirection,
        "bearing" -> stop.bearing,
        "validityPeriod" -> stop.validityPeriod,
        "floating" -> stop.floating)
    }
  }

  delete("/massTransitStops/removal") {
    val user = userProvider.getCurrentUser()
    val assetId = (parsedBody \ "assetId").extractOpt[Int].get
    val municipalityCode = linearAssetService.getMunicipalityCodeByAssetId(assetId)
    validateUserMunicipalityAccess(user)(municipalityCode)
    if(!user.isBusStopMaintainer()){
      halt(MethodNotAllowed("User not authorized, User needs to be BusStopMaintainer for do that action."))
    }
    else {
      massTransitStopService.deleteAllMassTransitStopData(assetId)
    }
  }

  get("/user/roles") {
    userProvider.getCurrentUser().configuration.roles
  }

  get("/massTransitStops/:nationalId") {
    def validateMunicipalityAuthorization(nationalId: Long)(municipalityCode: Int): Unit = {
      if (!userProvider.getCurrentUser().isAuthorizedToRead(municipalityCode))
        halt(Unauthorized("User not authorized for mass transit stop " + nationalId))
    }
    val nationalId = params("nationalId").toLong
    val massTransitStopReturned = massTransitStopService.getMassTransitStopByNationalIdWithTRWarnings(nationalId, validateMunicipalityAuthorization(nationalId))
    val massTransitStop = massTransitStopReturned._1.map { stop =>
      Map("id" -> stop.id,
        "nationalId" -> stop.nationalId,
        "stopTypes" -> stop.stopTypes,
        "lat" -> stop.lat,
        "lon" -> stop.lon,
        "validityDirection" -> stop.validityDirection,
        "bearing" -> stop.bearing,
        "validityPeriod" -> stop.validityPeriod,
        "floating" -> stop.floating,
        "propertyData" -> stop.propertyData)
    }

    if (massTransitStopReturned._2) {
      TierekisteriNotFoundWarning(massTransitStop.getOrElse(NotFound("Mass transit stop " + nationalId + " not found")))
    } else {
      massTransitStop.getOrElse(NotFound("Mass transit stop " + nationalId + " not found"))
    }
  }

/**
Returns empty result as Json message, not as page not found
*/
    get("/massTransitStopsSafe/:nationalId") {
      def validateMunicipalityAuthorization(nationalId: Long)(municipalityCode: Int): Unit = {
        if (!userProvider.getCurrentUser().isAuthorizedToRead(municipalityCode))
          halt(Unauthorized("User not authorized for mass transit stop " + nationalId))
      }
      val nationalId = params("nationalId").toLong
      val massTransitStopReturned =massTransitStopService.getMassTransitStopByNationalIdWithTRWarnings(nationalId, validateMunicipalityAuthorization(nationalId))
      massTransitStopReturned._1 match {
        case Some(stop) =>
          Map ("id" -> stop.id,
            "nationalId" -> stop.nationalId,
            "stopTypes" -> stop.stopTypes,
            "lat" -> stop.lat,
            "lon" -> stop.lon,
            "validityDirection" -> stop.validityDirection,
            "bearing" -> stop.bearing,
            "validityPeriod" -> stop.validityPeriod,
            "floating" -> stop.floating,
            "propertyData" -> stop.propertyData,
            "success" -> true)
        case None =>
          Map("success" -> false)
      }
    }

  get("/massTransitStops/livi/:liviId") {
    def validateMunicipalityAuthorization(id: String)(municipalityCode: Int): Unit = {
      if (!userProvider.getCurrentUser().isAuthorizedToRead(municipalityCode))
        halt(Unauthorized("User not authorized for mass transit stop " + id))
    }
    val liviId = params("liviId")
    val massTransitStopReturned = massTransitStopService.getMassTransitStopByLiviId(liviId, validateMunicipalityAuthorization(liviId))

    val massTransitStop = massTransitStopReturned.map { stop =>
      Map("id" -> stop.id,
        "nationalId" -> stop.nationalId,
        "stopTypes" -> stop.stopTypes,
        "lat" -> stop.lat,
        "lon" -> stop.lon,
        "validityDirection" -> stop.validityDirection,
        "bearing" -> stop.bearing,
        "validityPeriod" -> stop.validityPeriod,
        "floating" -> stop.floating,
        "propertyData" -> stop.propertyData,
        "success" -> true)
    }
    massTransitStop.getOrElse(Map("success" -> false))
  }

  get("/massTransitStops/floating") {
    val user = userProvider.getCurrentUser()
    val includedMunicipalities = user.isOperator() match {
      case true => None
      case false => Some(user.configuration.authorizedMunicipalities)
    }
    massTransitStopService.getFloatingAssetsWithReason(includedMunicipalities, Some(user.isOperator()))
  }

  get("/enumeratedPropertyValues/:assetTypeId") {
    assetPropertyService.getEnumeratedPropertyValues(params("assetTypeId").toLong)
  }

  private def massTransitStopPositionParameters(parsedBody: JValue): (Option[Double], Option[Double], Option[Long], Option[Int]) = {
    val lon = (parsedBody \ "lon").extractOpt[Double]
    val lat = (parsedBody \ "lat").extractOpt[Double]
    val roadLinkId = (parsedBody \ "linkId").extractOpt[Long]
    val bearing = (parsedBody \ "bearing").extractOpt[Int]
    (lon, lat, roadLinkId, bearing)
  }

  put("/massTransitStops/:id") {
    def validateMunicipalityAuthorization(id: Long)(municipalityCode: Int): Unit = {
      if (!userProvider.getCurrentUser().isAuthorizedToWrite(municipalityCode))
        halt(Unauthorized("User cannot update mass transit stop " + id + ". No write access to municipality " + municipalityCode))
    }
    val (optionalLon, optionalLat, optionalLinkId, bearing) = massTransitStopPositionParameters(parsedBody)
    val properties = (parsedBody \ "properties").extractOpt[Seq[SimpleProperty]].getOrElse(Seq())
    val linkId = (parsedBody \ "linkId").extractOpt[Long]
    validateBusStopMaintainerUser(properties)
    if(properties.exists(prop => prop.publicId == "vaikutussuunta")) {
      validateBusStopDirections(properties, linkId.get)
    }
    val position = (optionalLon, optionalLat, optionalLinkId) match {
      case (Some(lon), Some(lat), Some(linkId)) => Some(Position(lon, lat, linkId, bearing))
      case _ => None
    }
    try {
      val id = params("id").toLong
      massTransitStopService.updateExistingById(id, position, properties.toSet, userProvider.getCurrentUser().username, validateMunicipalityAuthorization(id))
    } catch {
      case e: NoSuchElementException => BadRequest("Target roadlink not found")
      case e: RoadAddressException =>
        logger.warn("RoadAddress error: " + e.getMessage)
        PreconditionFailed("Unable to find target road link")
    }
  }

  private def createMassTransitStop(lon: Double, lat: Double, linkId: Long, bearing: Int, properties: Seq[SimpleProperty]): Long = {
    val roadLink = vvhClient.fetchByLinkId(linkId).getOrElse(throw new NoSuchElementException)
    massTransitStopService.create(NewMassTransitStop(lon, lat, linkId, bearing, properties), userProvider.getCurrentUser().username, roadLink.geometry, roadLink.municipalityCode, Some(roadLink.administrativeClass))
  }

  private def validateUserRights(linkId: Long) = {
    val authorized: Boolean = vvhClient.fetchByLinkId(linkId).map(_.municipalityCode).exists(userProvider.getCurrentUser().isAuthorizedToWrite)
    if (!authorized) halt(Unauthorized("User not authorized"))
  }

  private def validateBusStopMaintainerUser(properties: Seq[SimpleProperty]) = {
    val user = userProvider.getCurrentUser()
    val propertyToValidation = properties.find {
      property => property.publicId.equals("tietojen_yllapitaja") && property.values.exists(p => p.propertyValue.equals("2"))
    }
    if ((propertyToValidation.size >= 1) && (!user.isBusStopMaintainer())) {
      halt(MethodNotAllowed("User not authorized, User needs to be BusStopMaintainer for do that action."))
    }
  }

  private def validateCreationProperties(properties: Seq[SimpleProperty]) = {
    val mandatoryProperties: Map[String, String] = massTransitStopService.mandatoryProperties()
    val nonEmptyMandatoryProperties: Seq[SimpleProperty] = properties.filter { property =>
      mandatoryProperties.contains(property.publicId) && property.values.nonEmpty
    }
    val missingProperties: Set[String] = mandatoryProperties.keySet -- nonEmptyMandatoryProperties.map(_.publicId).toSet
    if (missingProperties.nonEmpty) halt(BadRequest("Missing mandatory properties: " + missingProperties.mkString(", ")))
    val propertiesWithInvalidValues = nonEmptyMandatoryProperties.filter { property =>
      val propertyType = mandatoryProperties(property.publicId)
      propertyType match {
        case PropertyTypes.MultipleChoice =>
          property.values.forall { value => isBlank(value.propertyValue) || value.propertyValue.toInt == 99 }
        case _ =>
          property.values.forall { value => isBlank(value.propertyValue) }
      }
    }
    if (propertiesWithInvalidValues.nonEmpty)
      halt(BadRequest("Invalid property values on: " + propertiesWithInvalidValues.map(_.publicId).mkString(", ")))
  }

  private def validateBusStopDirections(properties: Seq[SimpleProperty], linkId: Long) = {
    val roadLink = roadLinkService.getRoadLinkFromVVH(linkId)
    val roadLinkDirection = roadLink.map(dir => dir.trafficDirection).headOption
    val massDirection = massTransitStopService

    val busStopDirection = properties.find(prop => prop.publicId == "vaikutussuunta")
                                     .get.values
                                     .map(dir => dir.propertyValue).head
    if((roadLinkDirection.head.toString != SideCode.BothDirections.toString) && (roadLinkDirection.head.toString != SideCode.apply(busStopDirection.toInt).toString))
      halt(NotAcceptable("Invalid Mass Transit Stop direction"))
  }

  post("/massTransitStops") {
    val positionParameters = massTransitStopPositionParameters(parsedBody)
    val lon = positionParameters._1.get
    val lat = positionParameters._2.get
    val linkId = positionParameters._3.get
    val bearing = positionParameters._4.get
    val properties = (parsedBody \ "properties").extract[Seq[SimpleProperty]]
    validateUserRights(linkId)
    validateBusStopMaintainerUser(properties)
    validateCreationProperties(properties)
    validateBusStopDirections(properties, linkId)
    try {
      val id = createMassTransitStop(lon, lat, linkId, bearing, properties)
      massTransitStopService.getById(id)
    } catch {
      case e: RoadAddressException =>
        logger.warn(e.getMessage)
        PreconditionFailed("Unable to find target road link")
    }
  }

  private def getRoadLinksFromVVH(municipalities: Set[Int])(bbox: String): Seq[Seq[Map[String, Any]]] = {
    val boundingRectangle = constructBoundingRectangle(bbox)
    validateBoundingBox(boundingRectangle)
    val roadLinks = roadLinkService.withRoadAddress(roadLinkService.getRoadLinksFromVVH(boundingRectangle, municipalities))
    val partitionedRoadLinks = RoadLinkPartitioner.partition(roadLinks)
    partitionedRoadLinks.map {
      _.map(roadLinkToApi)
    }
  }

  private def getRoadlinksWithComplementaryFromVVH(municipalities: Set[Int])(bbox: String): Seq[Seq[Map[String, Any]]] = {
    val boundingRectangle = constructBoundingRectangle(bbox)
    validateBoundingBox(boundingRectangle)
    val roadLinks = roadLinkService.getRoadLinksWithComplementaryFromVVH(boundingRectangle, municipalities)
    val partitionedRoadLinks = RoadLinkPartitioner.partition(roadLinks)
    partitionedRoadLinks.map {
      _.map(roadLinkToApi)
    }
  }

  private def getRoadLinksHistoryFromVVH(municipalities: Set[Int])(bbox: String): Seq[Seq[Map[String, Any]]] = {
    val boundingRectangle = constructBoundingRectangle(bbox)
    validateBoundingBox(boundingRectangle)
    val roadLinks = roadLinkService.getRoadLinksHistoryFromVVH(boundingRectangle, municipalities)

    val partitionedRoadLinks = RoadLinkPartitioner.partition(roadLinks)
    partitionedRoadLinks.map {
      _.map(roadLinkToApi)
    }
  }

  def roadLinkToApi(roadLink: RoadLink): Map[String, Any] = {
    Map(
      "linkId" -> roadLink.linkId,
      "mmlId" -> roadLink.attributes.get("MTKID"),
      "points" -> roadLink.geometry,
      "administrativeClass" -> roadLink.administrativeClass.toString,
      "linkType" -> roadLink.linkType.value,
      "functionalClass" -> roadLink.functionalClass,
      "trafficDirection" -> roadLink.trafficDirection.toString,
      "modifiedAt" -> roadLink.modifiedAt,
      "modifiedBy" -> roadLink.modifiedBy,
      "municipalityCode" -> roadLink.attributes.get("MUNICIPALITYCODE"),
      "verticalLevel" -> roadLink.attributes.get("VERTICALLEVEL"),
      "roadNameFi" -> roadLink.attributes.get("ROADNAME_FI"),
      "roadNameSe" -> roadLink.attributes.get("ROADNAME_SE"),
      "roadNameSm" -> roadLink.attributes.get("ROADNAME_SM"),
      "minAddressNumberRight" -> roadLink.attributes.get("FROM_RIGHT"),
      "maxAddressNumberRight" -> roadLink.attributes.get("TO_RIGHT"),
      "minAddressNumberLeft" -> roadLink.attributes.get("FROM_LEFT"),
      "maxAddressNumberLeft" -> roadLink.attributes.get("TO_LEFT"),
      "roadPartNumber" -> extractLongValue(roadLink, "VIITE_ROAD_PART_NUMBER"),
      "roadNumber" -> extractLongValue(roadLink, "VIITE_ROAD_NUMBER"),
      "constructionType" -> roadLink.constructionType.value,
      "linkSource" -> roadLink.linkSource.value,
      "track" -> extractIntValue(roadLink, "VIITE_TRACK"),
      "startAddrMValue" -> extractLongValue(roadLink, "VIITE_START_ADDR"),
      "endAddrMValue" ->  extractLongValue(roadLink, "VIITE_END_ADDR")
    )
  }

  private def extractIntValue(roadLink: RoadLink, value: String) = {
    roadLink.attributes.get(value) match {
      case Some(x) => x.asInstanceOf[Int]
      case _ => None
    }
  }

  private def extractLongValue(roadLink: RoadLink, value: String) = {
    roadLink.attributes.get(value) match {
      case Some(x) => x.asInstanceOf[Long]
      case _ => None
    }
  }

  get("/roadlinks") {
    response.setHeader("Access-Control-Allow-Headers", "*")
    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator() || user.isBusStopMaintainer()) Set() else user.configuration.authorizedMunicipalities

    params.get("bbox")
      .map(getRoadLinksFromVVH(municipalities))
      .getOrElse(BadRequest("Missing mandatory 'bbox' parameter"))
  }

    get("/roadlinks/:linkId") {
      val linkId = params("linkId").toLong
      roadLinkService.getRoadLinkMiddlePointByLinkId(linkId).map {
        case (id, middlePoint,source) => Map("success"->true, "id" -> id, "middlePoint" -> middlePoint, "source" -> source.value)
      }.getOrElse(Map("success:" ->false, "Reason"->"Link-id not found or invalid input"))
    }

  get("/roadlinks/history") {
    response.setHeader("Access-Control-Allow-Headers", "*")

    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator()) Set() else user.configuration.authorizedMunicipalities

    params.get("bbox")
      .map(getRoadLinksHistoryFromVVH(municipalities))
      .getOrElse(BadRequest("Missing mandatory 'bbox' parameter"))
  }

  get("/roadlinks/mml/:mmlId") {
    val mmlId = params("mmlId").toLong
    roadLinkService.getRoadLinkMiddlePointByMmlId(mmlId).map {
      case (id, middlePoint) => Map("id" -> id, "middlePoint" -> middlePoint)
    }.getOrElse(NotFound("Road link with MML ID " + mmlId + " not found"))
  }

  get("/roadlinks/adjacent/:id") {
    val id = params("id").toLong
    roadLinkService.getAdjacent(id).map(roadLinkToApi)
  }

  get("/roadlinks/adjacents/:ids") {
    val ids = params("ids").split(',').map(_.toLong)
    roadLinkService.getAdjacents(ids.toSet).mapValues(_.map(roadLinkToApi))
  }

  get("/roadLinks/incomplete") {
    val user = userProvider.getCurrentUser()
    val includedMunicipalities = user.isOperator() match {
      case true => None
      case false => Some(user.configuration.authorizedMunicipalities)
    }
    roadLinkService.getIncompleteLinks(includedMunicipalities)
  }

  get("/roadlinks/complementaries"){
    response.setHeader("Access-Control-Allow-Headers", "*")

    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator() || user.isBusStopMaintainer()) Set() else user.configuration.authorizedMunicipalities

    params.get("bbox")
      .map(getRoadlinksWithComplementaryFromVVH(municipalities))
      .getOrElse(BadRequest("Missing mandatory 'bbox' parameter"))
  }

  put("/linkproperties") {
    val properties = parsedBody.extract[Seq[LinkProperties]]
    val user = userProvider.getCurrentUser()
    def municipalityValidation(municipalityCode: Int) = validateUserMunicipalityAccess(user)(municipalityCode)
    properties.map { prop =>
      roadLinkService.updateLinkProperties(prop.linkId, prop.functionalClass, prop.linkType, prop.trafficDirection, Option(user.username), municipalityValidation).map { roadLink =>
        Map("linkId" -> roadLink.linkId,
          "points" -> roadLink.geometry,
          "administrativeClass" -> roadLink.administrativeClass.toString,
          "functionalClass" -> roadLink.functionalClass,
          "trafficDirection" -> roadLink.trafficDirection.toString,
          "modifiedAt" -> roadLink.modifiedAt,
          "modifiedBy" -> roadLink.modifiedBy,
          "linkType" -> roadLink.linkType.value)
      }.getOrElse(halt(NotFound("Road link with MML ID " + prop.linkId + " not found")))
    }
  }

  get("/assetTypeProperties/:assetTypeId") {
    try {
      val assetTypeId = params("assetTypeId").toLong
      assetPropertyService.availableProperties(assetTypeId)
    } catch {
      case e: Exception => BadRequest("Invalid asset type id: " + params("assetTypeId"))
    }
  }

  get("/assetPropertyNames/:language") {
    val lang = params("language")
    assetPropertyService.assetPropertyNames(lang)
  }

  object TierekisteriInternalServerError {
    def apply(body: Any = Unit, headers: Map[String, String] = Map.empty, reason: String = "") =
      ActionResult(ResponseStatus(HttpStatus.SC_FAILED_DEPENDENCY, reason), body, headers)
  }

  object TierekisteriNotFoundWarning {
    def apply(body: Any = Unit, headers: Map[String, String] = Map.empty, reason: String = "") =
      ActionResult(ResponseStatus(HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, reason), body, headers)
  }

  object RoadAddressNotFound {
    def apply(body: Any = Unit, headers: Map[String, String] = Map.empty, reason: String = "") =
      ActionResult(ResponseStatus(HttpStatus.SC_PRECONDITION_FAILED, reason), body, headers)
  }

  error {
    case ise: IllegalStateException => halt(InternalServerError("Illegal state: " + ise.getMessage))
    case ue: UnauthenticatedException => halt(Unauthorized("Not authenticated"))
    case unf: UserNotFoundException => halt(Forbidden(unf.username))
    case te: TierekisteriClientException => halt(TierekisteriInternalServerError("Tietojen tallentaminen/muokkaminen Tierekisterissa epäonnistui. Tehtyjä muutoksia ei tallennettu OTH:ssa"))
    case rae: RoadAddressException => halt(RoadAddressNotFound("Sovellus ei pysty tunnistamaan annetulle pysäkin sijainnille tieosoitetta. Pysäkin tallennus Tierekisterissä ja OTH:ssa epäonnistui"))
    case e: Exception =>
      logger.error("API Error", e)
      NewRelic.noticeError(e)
      halt(InternalServerError("API error"))
  }

  private def validateBoundingBox(bbox: BoundingRectangle): Unit = {
    val leftBottom = bbox.leftBottom
    val rightTop = bbox.rightTop
    val width = Math.abs(rightTop.x - leftBottom.x).toLong
    val height = Math.abs(rightTop.y - leftBottom.y).toLong
    if ((width * height) > MAX_BOUNDING_BOX) {
      halt(BadRequest("Bounding box was too big: " + bbox))
    }
  }

  private[this] def constructBoundingRectangle(bbox: String) = {
    val BBOXList = bbox.split(",").map(_.toDouble)
    BoundingRectangle(Point(BBOXList(0), BBOXList(1)), Point(BBOXList(2), BBOXList(3)))
  }

  get("/linearassets") {
    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator()) Set() else user.configuration.authorizedMunicipalities
    val typeId = params.getOrElse("typeId", halt(BadRequest("Missing mandatory 'typeId' parameter"))).toInt
    params.get("bbox").map { bbox =>
      val boundingRectangle = constructBoundingRectangle(bbox)
      validateBoundingBox(boundingRectangle)
      if(user.isServiceRoadMaintainer())
      linearAssetService.getByIntersectedBoundingBox(typeId,user.configuration.authorizedAreas.head, boundingRectangle, municipalities).map { links =>
        links.map { link =>
          Map(
            "id" -> (if (link.id == 0) None else Some(link.id)),
            "linkId" -> link.linkId,
            "sideCode" -> link.sideCode,
            "trafficDirection" -> link.trafficDirection,
            "value" -> link.value.map(_.toJson),
            "points" -> link.geometry,
            "expired" -> link.expired,
            "startMeasure" -> link.startMeasure,
            "endMeasure" -> link.endMeasure,
            "modifiedBy" -> link.modifiedBy,
            "modifiedAt" -> link.modifiedDateTime,
            "createdBy" -> link.createdBy,
            "createdAt" -> link.createdDateTime
          )
        }
      }
      else
      linearAssetService.getByBoundingBox(typeId, boundingRectangle, municipalities).map { links =>
        links.map { link =>
          Map(
            "id" -> (if (link.id == 0) None else Some(link.id)),
            "linkId" -> link.linkId,
            "sideCode" -> link.sideCode,
            "trafficDirection" -> link.trafficDirection,
            "value" -> link.value.map(_.toJson),
            "points" -> link.geometry,
            "expired" -> link.expired,
            "startMeasure" -> link.startMeasure,
            "endMeasure" -> link.endMeasure,
            "modifiedBy" -> link.modifiedBy,
            "modifiedAt" -> link.modifiedDateTime,
            "createdBy" -> link.createdBy,
            "createdAt" -> link.createdDateTime
          )
        }
      }
    } getOrElse {
      BadRequest("Missing mandatory 'bbox' parameter")
    }
  }

  private def extractLinearAssetValue(value: JValue): Option[Value] = {
    val numericValue = value.extractOpt[Int]
    val prohibitionParameter: Option[Seq[ProhibitionValue]] = value.extractOpt[Seq[ProhibitionValue]]
    val maintenanceRoadParameter: Option[Seq[Properties]] = value.extractOpt[Seq[Properties]]
    val textualParameter = value.extractOpt[String]

    val prohibition = prohibitionParameter match {
      case Some(Nil) => None
      case None => None
      case Some(x) => Some(Prohibitions(x))
    }

    val maintenanceRoad = maintenanceRoadParameter match {
      case Some(Nil) => None
      case None => None
      case Some(x) => Some(MaintenanceRoad(x))
    }

    numericValue
      .map(NumericValue)
      .orElse(textualParameter.map(TextualValue))
      .orElse(prohibition)
      .orElse(maintenanceRoad)
  }

  private def extractNewLinearAssets(typeId: Int, value: JValue) = {
    typeId match {
      case LinearAssetTypes.ExitNumberAssetTypeId | LinearAssetTypes.EuropeanRoadAssetTypeId =>
        value.extractOpt[Seq[NewTextualValueAsset]].getOrElse(Nil).map(x => NewLinearAsset(x.linkId, x.startMeasure, x.endMeasure, TextualValue(x.value), x.sideCode, 0, None))
      case LinearAssetTypes.ProhibitionAssetTypeId | LinearAssetTypes.HazmatTransportProhibitionAssetTypeId =>
        value.extractOpt[Seq[NewProhibition]].getOrElse(Nil).map(x => NewLinearAsset(x.linkId, x.startMeasure, x.endMeasure, Prohibitions(x.value), x.sideCode, 0, None))
      case LinearAssetTypes.MaintenanceRoadAssetTypeId =>
        value.extractOpt[Seq[NewMaintenanceRoad]].getOrElse(Nil).map(x =>NewLinearAsset(x.linkId, x.startMeasure, x.endMeasure, MaintenanceRoad(x.value), x.sideCode, 0, None))
      case _ =>
        value.extractOpt[Seq[NewNumericValueAsset]].getOrElse(Nil).map(x => NewLinearAsset(x.linkId, x.startMeasure, x.endMeasure, NumericValue(x.value), x.sideCode, 0, None))
    }
  }

  post("/linearassets") {
    val user = userProvider.getCurrentUser()
    val typeId = (parsedBody \ "typeId").extractOrElse[Int](halt(BadRequest("Missing mandatory 'typeId' parameter")))
    if (user.isServiceRoadMaintainer() && typeId!=serviceRoadTypeid)
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val valueOption = extractLinearAssetValue(parsedBody \ "value")
    val existingAssets = (parsedBody \ "ids").extract[Set[Long]]
    val newLinearAssets = extractNewLinearAssets(typeId, parsedBody \ "newLimits")
    val existingLinkIds = linearAssetService.getPersistedAssetsByIds(typeId, existingAssets).map(_.linkId)
    val linkIds = newLinearAssets.map(_.linkId) ++ existingLinkIds
    roadLinkService.fetchVVHRoadlinks(linkIds.toSet)
      .map(_.municipalityCode)
      .foreach(validateUserMunicipalityAccess(user))

    val updatedNumericalIds = if (valueOption.nonEmpty) {
      try {
        valueOption.map(linearAssetService.update(existingAssets.toSeq, _, user.username)).getOrElse(Nil)
      } catch {
        case e: MissingMandatoryPropertyException => halt(BadRequest("Missing Mandatory Properties: " + e.missing.mkString(",")))
        case e: IllegalArgumentException => halt(BadRequest("Property not found"))
      }
    } else {
      linearAssetService.clearValue(existingAssets.toSeq, user.username)
    }

    try {
      val createdIds = linearAssetService.create(newLinearAssets, typeId, user.username)
      updatedNumericalIds ++ createdIds
    } catch {
      case e: MissingMandatoryPropertyException => halt(BadRequest("Missing Mandatory Properties: " + e.missing.mkString(",")))
    }
  }

  delete("/linearassets") {
    val user = userProvider.getCurrentUser()
    val ids = (parsedBody \ "ids").extract[Set[Long]]
    val typeId = (parsedBody \ "typeId").extractOrElse[Int](halt(BadRequest("Missing mandatory 'typeId' parameter")))
    if (user.isServiceRoadMaintainer() && typeId!=serviceRoadTypeid)
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val linkIds = linearAssetService.getPersistedAssetsByIds(typeId, ids).map(_.linkId)
    roadLinkService.fetchVVHRoadlinks(linkIds.toSet)
      .map(_.municipalityCode)
      .foreach(validateUserMunicipalityAccess(user))

    linearAssetService.expire(ids.toSeq, user.username)
  }

  post("/linearassets/:id") {
    val user = userProvider.getCurrentUser()
    val typeId = (parsedBody \ "typeId").extractOrElse[Int](halt(BadRequest("Missing mandatory 'typeId' parameter")))
    if (user.isServiceRoadMaintainer() && typeId!=serviceRoadTypeid)
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    linearAssetService.split(params("id").toLong,
      (parsedBody \ "splitMeasure").extract[Double],
      extractLinearAssetValue(parsedBody \ "existingValue"),
      extractLinearAssetValue(parsedBody \ "createdValue"),
      user.username,
      validateUserMunicipalityAccess(user))
  }

  post("/linearassets/:id/separate") {
    val user = userProvider.getCurrentUser()
    val typeId = (parsedBody \ "typeId").extractOrElse[Int](halt(BadRequest("Missing mandatory 'typeId' parameter")))
    if (user.isServiceRoadMaintainer() && typeId!=serviceRoadTypeid)
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    linearAssetService.separate(params("id").toLong,
      extractLinearAssetValue(parsedBody \ "valueTowardsDigitization"),
      extractLinearAssetValue(parsedBody \ "valueAgainstDigitization"),
      user.username,
      validateUserMunicipalityAccess(user))
  }

  get("/speedlimit/sid/") {
    val segmentID = params.get("segmentid").getOrElse(halt(BadRequest("Bad coordinates")))
    val speedLimit = speedLimitService.find(segmentID.toLong)
    speedLimit match {
      case Some(speedLimit) => {
        roadLinkService.getRoadLinkMiddlePointByLinkId(speedLimit.linkId) match {
          case Some(location) => {
            Map ("success" -> true,
              "linkId" ->  speedLimit.linkId,
              "latitude" -> location._2.y,
              "longitude"-> location._2.x
            )
          }
          case None => {
            Map("success" -> false)
          }
        }
      }
      case None => {
        Map("success" -> false)
      }
    }
  }

  get("/speedlimits") {
    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator()) Set() else user.configuration.authorizedMunicipalities

    params.get("bbox").map { bbox =>
      val boundingRectangle = constructBoundingRectangle(bbox)
      validateBoundingBox(boundingRectangle)
      speedLimitService.get(boundingRectangle, municipalities).map { linkPartition =>
        linkPartition.map { link =>
          Map(
            "id" -> (if (link.id == 0) None else Some(link.id)),
            "linkId" -> link.linkId,
            "sideCode" -> link.sideCode,
            "trafficDirection" -> link.trafficDirection,
            "value" -> link.value.map(_.value),
            "points" -> link.geometry,
            "startMeasure" -> link.startMeasure,
            "endMeasure" -> link.endMeasure,
            "modifiedBy" -> link.modifiedBy,
            "modifiedAt" -> link.modifiedDateTime,
            "createdBy" -> link.createdBy,
            "createdAt" -> link.createdDateTime
          )
        }
      }
    } getOrElse {
      BadRequest("Missing mandatory 'bbox' parameter")
    }
  }

  get("/speedlimits/history") {
    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator()) Set() else user.configuration.authorizedMunicipalities

    params.get("bbox").map { bbox =>
      val boundingRectangle = constructBoundingRectangle(bbox)
      validateBoundingBox(boundingRectangle)
      speedLimitService.getHistory(boundingRectangle, municipalities).map { linkPartition =>
        linkPartition.map { link =>
          Map(
            "id" -> (if (link.id == 0) None else Some(link.id)),
            "linkId" -> link.linkId,
            "sideCode" -> link.sideCode,
            "trafficDirection" -> link.trafficDirection,
            "value" -> link.value.map(_.value),
            "points" -> link.geometry,
            "startMeasure" -> link.startMeasure,
            "endMeasure" -> link.endMeasure,
            "modifiedBy" -> link.modifiedBy,
            "modifiedAt" -> link.modifiedDateTime,
            "createdBy" -> link.createdBy,
            "createdAt" -> link.createdDateTime
          )
        }
      }
    } getOrElse {
      BadRequest("Missing mandatory 'bbox' parameter")
    }
  }

  get("/speedlimits/unknown") {
    val user = userProvider.getCurrentUser()
    val includedMunicipalities = user.isOperator() match {
      case true => None
      case false => Some(user.configuration.authorizedMunicipalities)
    }
    speedLimitService.getUnknown(includedMunicipalities)
  }

  put("/speedlimits") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val optionalValue = (parsedBody \ "value").extractOpt[Int]
    val ids = (parsedBody \ "ids").extract[Seq[Long]]
    val newLimits = (parsedBody \ "newLimits").extract[Seq[NewLimit]]
    optionalValue match {
      case Some(value) =>
        val updatedIds = speedLimitService.updateValues(ids, value, user.username, validateUserMunicipalityAccess(user))
        val createdIds = speedLimitService.create(newLimits, value, user.username, validateUserMunicipalityAccess(user))
        speedLimitService.get(updatedIds ++ createdIds)
      case _ => BadRequest("Speed limit value not provided")
    }
  }

  post("/speedlimits/:speedLimitId/split") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    speedLimitService.split(params("speedLimitId").toLong,
      (parsedBody \ "splitMeasure").extract[Double],
      (parsedBody \ "existingValue").extract[Int],
      (parsedBody \ "createdValue").extract[Int],
      user.username,
      validateUserMunicipalityAccess(user))
  }

  post("/speedlimits/:speedLimitId/separate") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    speedLimitService.separate(params("speedLimitId").toLong,
      (parsedBody \ "valueTowardsDigitization").extract[Int],
      (parsedBody \ "valueAgainstDigitization").extract[Int],
      user.username,
      validateUserMunicipalityAccess(user))
  }

  post("/speedlimits") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val newLimit = NewLimit((parsedBody \ "linkId").extract[Long],
      (parsedBody \ "startMeasure").extract[Double],
      (parsedBody \ "endMeasure").extract[Double])

    speedLimitService.create(Seq(newLimit),
      (parsedBody \ "value").extract[Int],
      user.username,
      validateUserMunicipalityAccess(user)).headOption match {
      case Some(id) => speedLimitService.find(id)
      case _ => BadRequest("Speed limit creation failed")
    }
  }

  private def validateUserMunicipalityAccess(user: User)(municipality: Int): Unit = {
    if (!user.isServiceRoadMaintainer())
    if (!user.hasEarlyAccess() || !user.isAuthorizedToWrite(municipality)) {
      halt(Unauthorized("User not authorized"))
    }
  }

  get("/manoeuvres") {
    val user = userProvider.getCurrentUser()
    val municipalities: Set[Int] = if (user.isOperator()) Set() else user.configuration.authorizedMunicipalities
    params.get("bbox").map { bbox =>
      val boundingRectangle = constructBoundingRectangle(bbox)
      validateBoundingBox(boundingRectangle)
      manoeuvreService.getByBoundingBox(boundingRectangle, municipalities)
    } getOrElse {
      BadRequest("Missing mandatory 'bbox' parameter")
    }
  }

  post("/manoeuvres") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val manoeuvres = (parsedBody \ "manoeuvres").extractOrElse[Seq[NewManoeuvre]](halt(BadRequest("Malformed 'manoeuvres' parameter")))

    val manoeuvreIds = manoeuvres.map { manoeuvre =>

      val linkIds = manoeuvres.flatMap(_.linkIds)
      val roadlinks = roadLinkService.getRoadLinksByLinkIdsFromVVH(linkIds.toSet)

      roadlinks.map(_.municipalityCode)
        .foreach(validateUserMunicipalityAccess(user))

      if(!manoeuvreService.isValid(manoeuvre, roadlinks))
        halt(BadRequest("Invalid 'manouevre'"))

      manoeuvreService.createManoeuvre(user.username, manoeuvre)
    }
    Created(manoeuvreIds)
  }


  delete("/manoeuvres") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val manoeuvreIds = (parsedBody \ "manoeuvreIds").extractOrElse[Seq[Long]](halt(BadRequest("Malformed 'manoeuvreIds' parameter")))

    manoeuvreIds.foreach { manoeuvreId =>
      val sourceRoadLinkId = manoeuvreService.getSourceRoadLinkIdById(manoeuvreId)
      validateUserMunicipalityAccess(user)(vvhClient.fetchByLinkId(sourceRoadLinkId).get.municipalityCode)
      manoeuvreService.deleteManoeuvre(user.username, manoeuvreId)
    }
  }

  put("/manoeuvres") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val manoeuvreUpdates: Map[Long, ManoeuvreUpdates] = parsedBody
      .extractOrElse[Map[String, ManoeuvreUpdates]](halt(BadRequest("Malformed body on put manoeuvres request")))
      .map { case (id, updates) => (id.toLong, updates) }

    manoeuvreUpdates.foreach { case (id, updates) =>
      val sourceRoadLinkId = manoeuvreService.getSourceRoadLinkIdById(id)
      validateUserMunicipalityAccess(user)(vvhClient.fetchByLinkId(sourceRoadLinkId).get.municipalityCode)
      manoeuvreService.updateManoeuvre(user.username, id, updates)
    }
  }

  get("/pedestrianCrossings")(getPointAssets(pedestrianCrossingService))
  get("/pedestrianCrossings/:id")(getPointAssetById(pedestrianCrossingService))
  get("/pedestrianCrossings/floating")(getFloatingPointAssets(pedestrianCrossingService))
  delete("/pedestrianCrossings/:id")(deletePointAsset(pedestrianCrossingService))
  put("/pedestrianCrossings/:id")(updatePointAsset(pedestrianCrossingService))
  post("/pedestrianCrossings")(createNewPointAsset(pedestrianCrossingService))

  get("/obstacles")(getPointAssets(obstacleService))
  get("/obstacles/:id")(getPointAssetById(obstacleService))
  get("/obstacles/floating")(getFloatingPointAssets(obstacleService))
  delete("/obstacles/:id")(deletePointAsset(obstacleService))
  put("/obstacles/:id")(updatePointAsset(obstacleService))
  post("/obstacles")(createNewPointAsset(obstacleService))

  get("/railwayCrossings")(getPointAssets(railwayCrossingService))
  get("/railwayCrossings/:id")(getPointAssetById(railwayCrossingService))
  get("/railwayCrossings/floating")(getFloatingPointAssets(railwayCrossingService))
  put("/railwayCrossings/:id")(updatePointAsset(railwayCrossingService))
  delete("/railwayCrossings/:id")(deletePointAsset(railwayCrossingService))
  post("/railwayCrossings")(createNewPointAsset(railwayCrossingService))

  get("/directionalTrafficSigns")(getPointAssets(directionalTrafficSignService))
  get("/directionalTrafficSigns/:id")(getPointAssetById(directionalTrafficSignService))
  get("/directionalTrafficSigns/floating")(getFloatingPointAssets(directionalTrafficSignService))
  post("/directionalTrafficSigns")(createNewPointAsset(directionalTrafficSignService))
  put("/directionalTrafficSigns/:id")(updatePointAsset(directionalTrafficSignService))
  delete("/directionalTrafficSigns/:id")(deletePointAsset(directionalTrafficSignService))

  get("/trafficLights")(getPointAssets(trafficLightService))
  get("/trafficLights/:id")(getPointAssetById(trafficLightService))
  get("/trafficLights/floating")(getFloatingPointAssets(trafficLightService))
  post("/trafficLights")(createNewPointAsset(trafficLightService))
  put("/trafficLights/:id")(updatePointAsset(trafficLightService))
  delete("/trafficLights/:id")(deletePointAsset(trafficLightService))

  private def getPointAssets(service: PointAssetOperations): Seq[service.PersistedAsset] = {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val bbox = params.get("bbox").map(constructBoundingRectangle).getOrElse(halt(BadRequest("Bounding box was missing")))
    validateBoundingBox(bbox)
    service.getByBoundingBox(user, bbox)
  }

  private def getPointAssetById(service: PointAssetOperations) = {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val asset = service.getById(params("id").toLong)
    asset match {
      case None => halt(NotFound("Asset with given id not found"))
      case Some(foundAsset) =>
        validateUserMunicipalityAccess(user)(foundAsset.municipalityCode)
        foundAsset
    }
  }

  private def getFloatingPointAssets(service: PointAssetOperations) = {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val includedMunicipalities = user.isOperator() match {
      case true => None
      case false => Some(user.configuration.authorizedMunicipalities)
    }
    service.getFloatingAssets(includedMunicipalities)
  }

  private def deletePointAsset(service: PointAssetOperations): Long = {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val id = params("id").toLong
    service.getPersistedAssetsByIds(Set(id)).headOption.map(_.municipalityCode).foreach(validateUserMunicipalityAccess(user))
    service.expire(id, user.username)
  }

  private def updatePointAsset(service: PointAssetOperations)(implicit m: Manifest[service.IncomingAsset]) {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val id = params("id").toLong
    val updatedAsset = (parsedBody \ "asset").extract[service.IncomingAsset]
    roadLinkService.getRoadLinkFromVVH(updatedAsset.linkId) match {
      case None => halt(NotFound(s"Roadlink with mml id ${updatedAsset.linkId} does not exist"))
      case Some(link) => service.update(id, updatedAsset, link.geometry, link.municipalityCode, user.username)
    }
  }

  private def createNewPointAsset(service: PointAssetOperations)(implicit m: Manifest[service.IncomingAsset]) = {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val asset = (parsedBody \ "asset").extract[service.IncomingAsset]
    for (link <- vvhClient.fetchByLinkId(asset.linkId)) {
      validateUserMunicipalityAccess(user)(link.municipalityCode)
      service.create(asset, user.username, link.geometry, link.municipalityCode, Some(link.administrativeClass))
    }
  }

  get("/servicePoints") {
    val bbox = params.get("bbox").map(constructBoundingRectangle).getOrElse(halt(BadRequest("Bounding box was missing")))
    servicePointService.get(bbox)
  }

  post("/servicePoints") {
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    val asset = (parsedBody \ "asset").extract[IncomingServicePoint]
    roadLinkService.getClosestRoadlinkFromVVH(user, Point(asset.lon, asset.lat)) match {
      case None =>
        halt(Conflict(s"Can not find nearby road link for given municipalities " + user.configuration.authorizedMunicipalities))
      case Some(link) =>
        servicePointService.create(asset, link.municipalityCode, user.username)
    }
  }

  put("/servicePoints/:id") {
    val id = params("id").toLong
    val updatedAsset = (parsedBody \ "asset").extract[IncomingServicePoint]
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    roadLinkService.getClosestRoadlinkFromVVH(user, Point(updatedAsset.lon, updatedAsset.lat)) match {
      case None =>
        halt(Conflict(s"Can not find nearby road link for given municipalities " + user.configuration.authorizedMunicipalities))
      case Some(link) =>
        servicePointService.update(id, updatedAsset, link.municipalityCode, user.username)
    }
  }

  delete("/servicePoints/:id") {
    val id = params("id").toLong
    val user = userProvider.getCurrentUser()
    if (user.isServiceRoadMaintainer())
      halt(Unauthorized("ServiceRoad user is only authorized to alter serviceroad assets"))
    servicePointService.expire(id, user.username)
  }
}
