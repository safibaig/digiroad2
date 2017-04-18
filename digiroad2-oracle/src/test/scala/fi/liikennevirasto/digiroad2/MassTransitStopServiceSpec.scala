package fi.liikennevirasto.digiroad2

import java.text.SimpleDateFormat
import java.util.Date

import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.linearasset.RoadLink
import fi.liikennevirasto.digiroad2.masstransitstop.MassTransitStopOperations
import fi.liikennevirasto.digiroad2.masstransitstop.oracle.MassTransitStopDao
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.user.{Configuration, User}
import fi.liikennevirasto.digiroad2.util._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.mockito.Matchers._
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{StaticQuery => Q}

class MassTransitStopServiceSpec extends FunSuite with Matchers with BeforeAndAfter {
  val geometryTransform = new GeometryTransform
  val boundingBoxWithKauniainenAssets = BoundingRectangle(Point(374000,6677000), Point(374800,6677600))
  val userWithKauniainenAuthorization = User(
    id = 1,
    username = "Hannu",
    configuration = Configuration(authorizedMunicipalities = Set(235)))
  val mockTierekisteriClient = MockitoSugar.mock[TierekisteriClient]
  when(mockTierekisteriClient.fetchMassTransitStop(any[String])).thenReturn(Some(
    TierekisteriMassTransitStop(2, "2", RoadAddress(None, 1, 1, Track.Combined, 1, None), TRRoadSide.Unknown, StopType.Combined,
      false, equipments = Map(), None, None, None, "KX12356", None, None, None, new Date))
  )

  val vvhRoadLinks = List(
    VVHRoadlink(1611353, 90, Nil, Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(1021227, 90, Nil, Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(1021226, 90, Nil, Private, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(123l, 91, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(12333l, 91, List(Point(0.0,0.0), Point(120.0, 0.0)), State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(131573L, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(6488445, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(1611353, 235, Seq(Point(374603.57,6677262.009), Point(374684.567, 6677277.323)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(1611341l, 91, Seq(Point(374375.156,6677244.904), Point(374567.632, 6677255.6)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers),
    VVHRoadlink(1l, 235, Seq(Point(0.0, 0.0), Point(10.0, 0.0)), Municipality, TrafficDirection.BothDirections, FeatureClass.AllOthers),
    VVHRoadlink(1611601L, 235, Seq(Point(374668.195,6676884.282), Point(374805.498, 6676906.051)), Municipality, TrafficDirection.BothDirections, FeatureClass.AllOthers))

  val mockVVHClient = MockitoSugar.mock[VVHClient]
  val mockGeometryTransform = MockitoSugar.mock[GeometryTransform]
  val mockRoadLinkService = MockitoSugar.mock[RoadLinkService]

  def toRoadLink(l: VVHRoadlink) = {
    RoadLink(l.linkId, l.geometry, GeometryUtils.geometryLength(l.geometry),
      l.administrativeClass, 1, l.trafficDirection, UnknownLinkType, None, None, l.attributes + ("MUNICIPALITYCODE" -> BigInt(l.municipalityCode)))
  }
  before {
    // Reset the mocks here so individual tests don't have to
    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)
    vvhRoadLinks.foreach(rl =>
      when(mockVVHClient.fetchByLinkId(rl.linkId))
        .thenReturn(Some(rl)))
    when(mockVVHClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockGeometryTransform.resolveAddressAndLocation(any[Double], any[Long], any[Int], any[Option[Int]], any[Option[Int]]
      )).thenReturn((RoadAddress(Option("235"), 1, 1, Track.Combined, 0, None), RoadSide.Right))
    when(mockRoadLinkService.getRoadLinksFromVVH(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks.map(toRoadLink))
    vvhRoadLinks.foreach(rl =>
      when(mockRoadLinkService.getRoadLinkFromVVH(rl.linkId, false))
        .thenReturn(Some(toRoadLink(rl))))
    when(mockRoadLinkService.fetchVVHRoadlinks(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockRoadLinkService.getRoadLinksByLinkIdsFromVVH(any[Set[Long]], any[Boolean])).thenReturn(vvhRoadLinks.map(toRoadLink))}
  class TestMassTransitStopService(val eventbus: DigiroadEventBus, val roadLinkService: RoadLinkService) extends MassTransitStopService {
    override def withDynSession[T](f: => T): T = f
    override def withDynTransaction[T](f: => T): T = f
    override val tierekisteriClient: TierekisteriClient = mockTierekisteriClient
    override val massTransitStopDao: MassTransitStopDao = new MassTransitStopDao
    override val tierekisteriEnabled = false
    override val geometryTransform: GeometryTransform = mockGeometryTransform
  }

  class TestMassTransitStopServiceWithTierekisteri(val eventbus: DigiroadEventBus, val roadLinkService: RoadLinkService) extends MassTransitStopService {
    override def withDynSession[T](f: => T): T = f
    override def withDynTransaction[T](f: => T): T = f
    override val tierekisteriClient: TierekisteriClient = mockTierekisteriClient
    override val massTransitStopDao: MassTransitStopDao = new MassTransitStopDao
    override val tierekisteriEnabled = true
    override val geometryTransform: GeometryTransform = mockGeometryTransform
  }

  object RollbackMassTransitStopService extends TestMassTransitStopService(new DummyEventBus, mockRoadLinkService)

  object RollbackMassTransitStopServiceWithTierekisteri extends TestMassTransitStopServiceWithTierekisteri(new DummyEventBus, mockRoadLinkService)

  class TestMassTransitStopServiceWithDynTransaction(val eventbus: DigiroadEventBus, val roadLinkService: RoadLinkService) extends MassTransitStopService {
    override def withDynSession[T](f: => T): T = TestTransactions.withDynSession()(f)
    override def withDynTransaction[T](f: => T): T = TestTransactions.withDynTransaction()(f)
    override val tierekisteriClient: TierekisteriClient = mockTierekisteriClient
    override val massTransitStopDao: MassTransitStopDao = new MassTransitStopDao
    override val tierekisteriEnabled = true
    override val geometryTransform: GeometryTransform = mockGeometryTransform
  }

  class MassTransitStopServiceWithTierekisteri(val eventbus: DigiroadEventBus, val roadLinkService: RoadLinkService) extends MassTransitStopService {
    override val tierekisteriClient: TierekisteriClient = mockTierekisteriClient
    override val massTransitStopDao: MassTransitStopDao = new MassTransitStopDao
    override val tierekisteriEnabled = true
    override val geometryTransform: GeometryTransform = mockGeometryTransform
  }

  def runWithRollback(test: => Unit): Unit = TestTransactions.runWithRollback()(test)

  val assetLock = "Used to prevent deadlocks"

  test("update inventory date") {
    val props = Seq(SimpleProperty("foo", Seq()))
    val after = RollbackMassTransitStopService.updatedProperties(props)
    after should have size (2)
    val after2 = RollbackMassTransitStopService.updatedProperties(after)
    after2 should have size (2)
  }

  test("update empty inventory date") {
    val props = Seq(SimpleProperty("inventointipaiva", Seq()))
    val after = RollbackMassTransitStopService.updatedProperties(props)
    after should have size (1)
    after.head.values should have size(1)
    after.head.values.head.propertyValue should be ( DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now))
  }

  test("do not update existing inventory date") {
    val props = Seq(SimpleProperty("inventointipaiva", Seq(PropertyValue("2015-12-30"))))
    val after = RollbackMassTransitStopService.updatedProperties(props)
    after should have size (1)
    after.head.values should have size(1)
    after.head.values.head.propertyValue should be ( "2015-12-30")
  }

  test("Calculate mass transit stop validity periods") {
    runWithRollback {
      assetLock.synchronized {
        val massTransitStops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBoxWithKauniainenAssets)
        massTransitStops.find(_.id == 300000).flatMap(_.validityPeriod) should be(Some(MassTransitStopValidityPeriod.Current))
        massTransitStops.find(_.id == 300001).flatMap(_.validityPeriod) should be(Some(MassTransitStopValidityPeriod.Past))
        massTransitStops.find(_.id == 300003).flatMap(_.validityPeriod) should be(Some(MassTransitStopValidityPeriod.Future))
      }
    }
  }

  test("Return mass transit stop types") {
    runWithRollback {
      assetLock.synchronized {
        val massTransitStops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBoxWithKauniainenAssets)
        massTransitStops.find(_.id == 300000).get.stopTypes should be(Seq(2))
        massTransitStops.find(_.id == 300001).get.stopTypes.sorted should be(Seq(2, 3, 4))
        massTransitStops.find(_.id == 300003).get.stopTypes.sorted should be(Seq(2, 3))
      }
    }
  }

  test("Get stops by bounding box") {
    runWithRollback {
      val vvhRoadLink = VVHRoadlink(11, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = RollbackMassTransitStopService.create(NewMassTransitStop(5.0, 0.0, 1l, 2, Nil), "masstransitstopservice_spec", vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      val stops = RollbackMassTransitStopService.getByBoundingBox(
        userWithKauniainenAuthorization, BoundingRectangle(Point(0.0, 0.0), Point(10.0, 10.0)))
      stops.map(_.id) should be(Seq(id))
    }
  }

  test("Filter stops by authorization") {
    runWithRollback {
      val stops = RollbackMassTransitStopService.getByBoundingBox(User(0, "test", Configuration()), boundingBoxWithKauniainenAssets)
      stops should be(empty)
    }
  }

  test("Stop floats if road link does not exist") {
    runWithRollback {
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBoxWithKauniainenAssets)
      stops.find(_.id == 300000).map(_.floating) should be(Some(true))
    }
  }

  test("Stop floats if stop and roadlink municipality codes differ") {
    runWithRollback {
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBoxWithKauniainenAssets)
      stops.find(_.id == 300004).map(_.floating) should be(Some(true))
    }
  }

  test("Stop floats if stop is too far from linearly referenced location") {
    runWithRollback {
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBoxWithKauniainenAssets)
      stops.find(_.id == 300008).map(_.floating) should be(Some(true))
    }
  }

  test("Persist mass transit stop floating status change") {
    runWithRollback {
      RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBoxWithKauniainenAssets)
      val floating: Option[Boolean] = sql"""select floating from asset where id = 300008""".as[Boolean].firstOption
      floating should be(Some(true))
    }
  }

  test("Fetch mass transit stop by national id") {
    runWithRollback {
      val equipments = Map[Equipment, Existence](
        Equipment.BikeStand -> Existence.Yes,
        Equipment.CarParkForTakingPassengers -> Existence.Unknown,
        Equipment.ElectronicTimetables -> Existence.Yes,
        Equipment.RaisedBusStop -> Existence.No,
        Equipment.Lighting -> Existence.Unknown,
        Equipment.Roof -> Existence.Yes,
        Equipment.Seat -> Existence.Unknown,
        Equipment.Timetable -> Existence.No,
        Equipment.TrashBin -> Existence.Yes,
        Equipment.RoofMaintainedByAdvertiser -> Existence.Yes
      )
      val roadAddress = RoadAddress(None, 0, 0, Track.Unknown, 0, None)
      when(mockTierekisteriClient.fetchMassTransitStop("OTHJ85755")).thenReturn(Some(
        TierekisteriMassTransitStop(85755, "OTHJ85755", roadAddress, TRRoadSide.Unknown, StopType.Unknown, false, equipments, None, Option("TierekisteriFi"), Option("TierekisteriSe"), "test", Option(new Date), Option(new Date), Option(new Date), new Date(2016,8,1))
      ))
      val (stop, showStatusCode) = RollbackMassTransitStopService.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)
      stop.map(_.floating) should be(Some(true))

      showStatusCode should be (false)
    }
  }

  test("Fetch mass transit stop enriched by tierekisteri by national id"){

    runWithRollback{
      val roadAddress = RoadAddress(None, 0, 0, Track.Unknown, 0, None)
      val equipments = Map[Equipment, Existence](
        Equipment.BikeStand -> Existence.Yes,
        Equipment.CarParkForTakingPassengers -> Existence.Unknown,
        Equipment.ElectronicTimetables -> Existence.Yes,
        Equipment.RaisedBusStop -> Existence.No,
        Equipment.Lighting -> Existence.Unknown,
        Equipment.Roof -> Existence.Yes,
        Equipment.Seat -> Existence.Unknown,
        Equipment.Timetable -> Existence.No,
        Equipment.TrashBin -> Existence.Yes,
        Equipment.RoofMaintainedByAdvertiser -> Existence.Yes
      )
      when(mockTierekisteriClient.fetchMassTransitStop("OTHJ85755")).thenReturn(Some(
        TierekisteriMassTransitStop(85755, "OTHJ85755", roadAddress, TRRoadSide.Unknown, StopType.Unknown, false, equipments, None, Option("TierekisteriFi"), Option("TierekisteriSe"), "test", Option(new Date), Option(new Date), Option(new Date), new Date(2016, 9, 2))
      ))

      val (stop, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)
      equipments.foreach{
        case (equipment, existence) if(equipment.isMaster) =>
          val property = stop.map(_.propertyData).get.find(p => p.publicId == equipment.publicId).get
          property.values should have size (1)
          property.values.head.propertyValue should be(existence.propertyValue.toString)
        case _ => ;
      }
      val name_fi = stop.get.propertyData.find(_.publicId == MassTransitStopOperations.nameFiPublicId).get.values
      val name_se = stop.get.propertyData.find(_.publicId == MassTransitStopOperations.nameSePublicId).get.values
      name_fi should have size (1)
      name_se should have size (1)
      name_fi.head.propertyValue should be ("TierekisteriFi")
      name_se.head.propertyValue should be ("TierekisteriSe")
      showStatusCode should be (false)
    }
  }

  test("Tierekisteri master overrides set values set to Yes"){

    runWithRollback{
      val roadAddress = RoadAddress(None, 0, 0, Track.Unknown, 0, None)
      val equipments = Map[Equipment, Existence](
        Equipment.BikeStand -> Existence.Yes,
        Equipment.CarParkForTakingPassengers -> Existence.Yes,
        Equipment.ElectronicTimetables -> Existence.Yes,
        Equipment.RaisedBusStop -> Existence.Yes,
        Equipment.Lighting -> Existence.Yes,
        Equipment.Roof -> Existence.Yes,
        Equipment.Seat -> Existence.Yes,
        Equipment.Timetable -> Existence.Yes,
        Equipment.TrashBin -> Existence.Yes,
        Equipment.RoofMaintainedByAdvertiser -> Existence.Yes
      )
      when(mockTierekisteriClient.fetchMassTransitStop("OTHJ85755")).thenReturn(Some(
        TierekisteriMassTransitStop(85755, "OTHJ85755", roadAddress, TRRoadSide.Unknown, StopType.Unknown, false, equipments, None, Option("TierekisteriFi"), Option("TierekisteriSe"), "test", Option(new Date), Option(new Date), Option(new Date), new Date(2016, 9, 2)))
      )

      val (stop, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)
      equipments.foreach{
        case (equipment, existence) if equipment.isMaster =>
          val property = stop.map(_.propertyData).get.find(p => p.publicId == equipment.publicId).get
          property.values should have size (1)
          property.values.head.propertyValue should be(existence.propertyValue.toString)
        case _ => ;
      }
      showStatusCode should be (false)
    }
  }

  test("Tierekisteri master overrides set values set to No"){

    runWithRollback{
      val roadAddress = RoadAddress(None, 0, 0, Track.Unknown, 0, None)
      val equipments = Map[Equipment, Existence](
        Equipment.BikeStand -> Existence.No,
        Equipment.CarParkForTakingPassengers -> Existence.No,
        Equipment.ElectronicTimetables -> Existence.No,
        Equipment.RaisedBusStop -> Existence.No,
        Equipment.Lighting -> Existence.No,
        Equipment.Roof -> Existence.No,
        Equipment.Seat -> Existence.No,
        Equipment.Timetable -> Existence.No,
        Equipment.TrashBin -> Existence.No,
        Equipment.RoofMaintainedByAdvertiser -> Existence.No
      )
      when(mockTierekisteriClient.fetchMassTransitStop("OTHJ85755")).thenReturn(Some(
        TierekisteriMassTransitStop(85755, "OTHJ85755", roadAddress, TRRoadSide.Unknown, StopType.Unknown, false, equipments, None, Option("TierekisteriFi"), Option("TierekisteriSe"), "test", Option(new Date), Option(new Date), Option(new Date), new Date(2016, 9, 2)))
      )

      val (stop, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)
      equipments.foreach{
        case (equipment, existence) if equipment.isMaster =>
          val property = stop.map(_.propertyData).get.find(p => p.publicId == equipment.publicId).get
          property.values should have size (1)
          property.values.head.propertyValue should be(existence.propertyValue.toString)
        case _ => ;
      }
      showStatusCode should be (false)
    }
  }

  test("Tierekisteri master overrides set values set to Unknown"){

    runWithRollback{
      val roadAddress = RoadAddress(None, 0, 0, Track.Unknown, 0, None)
      val equipments = Map[Equipment, Existence](
        Equipment.BikeStand -> Existence.Unknown,
        Equipment.CarParkForTakingPassengers -> Existence.Unknown,
        Equipment.ElectronicTimetables -> Existence.Unknown,
        Equipment.RaisedBusStop -> Existence.Unknown,
        Equipment.Lighting -> Existence.Unknown,
        Equipment.Roof -> Existence.Unknown,
        Equipment.Seat -> Existence.Unknown,
        Equipment.Timetable -> Existence.Unknown,
        Equipment.TrashBin -> Existence.Unknown,
        Equipment.RoofMaintainedByAdvertiser -> Existence.Unknown
      )
      when(mockTierekisteriClient.fetchMassTransitStop("OTHJ85755")).thenReturn(Some(
        TierekisteriMassTransitStop(85755, "OTHJ85755", roadAddress, TRRoadSide.Unknown, StopType.Unknown, false, equipments, None, Option("TierekisteriFi"), Option("TierekisteriSe"), "test", Option(new Date), Option(new Date), Option(new Date), new Date(2016, 9, 2)))
      )

      val (stop, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)
      equipments.foreach{
        case (equipment, existence) if equipment.isMaster =>
          val property = stop.map(_.propertyData).get.find(p => p.publicId == equipment.publicId).get
          property.values should have size (1)
          property.values.head.propertyValue should be(existence.propertyValue.toString)
        case _ => ;
      }
      showStatusCode should be (false)
    }
  }

  test("Fetch mass transit stop by national id but does not exist in Tierekisteri"){
    runWithRollback{

      when(mockTierekisteriClient.fetchMassTransitStop("OTHJ85755")).thenReturn(None)
      val (stop, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)

      stop.size should be (1)
      stop.map(_.nationalId).get should be (85755)
      showStatusCode should be (true)
    }
  }

  test("Get properties") {
    runWithRollback {
      val massTransitStop = RollbackMassTransitStopService.getMassTransitStopByNationalIdWithTRWarnings(2, Int => Unit)._1.map { stop =>
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
    }
  }

  test("Assert user rights when fetching mass transit stop with id") {
    runWithRollback {
      an [Exception] should be thrownBy RollbackMassTransitStopService.getMassTransitStopByNationalIdWithTRWarnings(85755, { municipalityCode => throw new Exception })
    }
  }

  test("Update mass transit stop road link mml id") {
    runWithRollback {
      assetLock.synchronized {
        val geom = Point(374450, 6677250)
        val position = Some(Position(geom.x, geom.y, 1611601L, Some(85)))
        RollbackMassTransitStopService.updateExistingById(300000, position, Set.empty, "user", _ => Unit)
        val linkId = sql"""
            select lrm.link_id from asset a
            join asset_link al on al.asset_id = a.id
            join lrm_position lrm on lrm.id = al.position_id
            where a.id = 300000
      """.as[Long].firstOption
        linkId should be(Some(1611601L))
      }
    }
  }

  test("Update mass transit stop bearing") {
    runWithRollback {
      assetLock.synchronized {
        val geom = Point(374450, 6677250)
        val position = Some(Position(geom.x, geom.y, 1611341l, Some(90)))
        RollbackMassTransitStopService.updateExistingById(300000, position, Set.empty, "user", _ => Unit)
        val bearing = sql"""
            select a.bearing from asset a
            join asset_link al on al.asset_id = a.id
            join lrm_position lrm on lrm.id = al.position_id
            where a.id = 300000
      """.as[Option[Int]].first
        bearing should be(Some(90))
      }
    }
  }

  test("Update mass transit stop municipality") {
    runWithRollback {
      assetLock.synchronized {
        val geom = Point(374450, 6677250)
        val position = Some(Position(geom.x, geom.y, 1611341l, Some(85)))
        RollbackMassTransitStopService.updateExistingById(300000, position, Set.empty, "user", _ => Unit)
        val municipality = sql"""
            select a.municipality_code from asset a
            join asset_link al on al.asset_id = a.id
            join lrm_position lrm on lrm.id = al.position_id
            where a.id = 300000
      """.as[Int].firstOption
        municipality should be(Some(91))
      }
    }
  }

  test("Do not overwrite asset liVi identifier property when already administered by ELY"){
    runWithRollback {
      assetLock.synchronized {
        val eventbus = MockitoSugar.mock[DigiroadEventBus]
        val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
        val assetId = 300000
        sqlu"""update text_property_value set value_fi='livi1' where asset_id = 300000 and value_fi = 'OTHJ1'""".execute
        val dbResult = sql"""SELECT value_fi FROM text_property_value where value_fi='livi1' and asset_id = 300000""".as[String].list
        dbResult.size should be(1)
        dbResult.head should be("livi1")
        val properties = List(
          SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
          SimpleProperty("yllapitajan_koodi", List(PropertyValue("OTHJ1"))))
        val position = Some(Position(374450, 6677250, 123l, None))
        RollbackMassTransitStopService.updateExistingById(assetId, position, properties.toSet, "user", _ => Unit)
        val massTransitStop = service.getById(assetId).get

        //The property yllapitajan_koodi should be overridden with OTHJ + NATIONAL ID
        val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
        liviIdentifierProperty.values.head.propertyValue should be("livi1")
      }
    }
  }

  test("Overwrite non-existent asset liVi identifier property when administered by ELY"){
    runWithRollback {
      assetLock.synchronized {
        val eventbus = MockitoSugar.mock[DigiroadEventBus]
        val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
        val assetId = 300000
        val propertyValueId = sql"""SELECT id FROM text_property_value where value_fi='OTHJ1' and asset_id = $assetId""".as[String].list.head
        sqlu"""update text_property_value set value_fi='' where id = $propertyValueId""".execute
        val dbResult = sql"""SELECT value_fi FROM text_property_value where id = $propertyValueId""".as[String].list
        dbResult.size should be(1)
        dbResult.head should be(null)
        val properties = List(
          SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
          SimpleProperty("yllapitajan_koodi", List(PropertyValue("OTHJ1"))))
        val position = Some(Position(374450, 6677250, 123l, None))
        RollbackMassTransitStopService.updateExistingById(assetId, position, properties.toSet, "user", _ => Unit)
        val massTransitStop = service.getById(assetId).get

        //The property yllapitajan_koodi should be overridden with OTHJ + NATIONAL ID
        val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
        liviIdentifierProperty.values.head.propertyValue should be("OTHJ1")
      }
    }
  }

  test("Update asset liVi identifier property when is NOT Central ELY administration"){
    runWithRollback {
      assetLock.synchronized {
        val eventbus = MockitoSugar.mock[DigiroadEventBus]
        val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
        val assetId = 300000
        val propertyValueId = sql"""SELECT id FROM text_property_value where value_fi='OTHJ1' and asset_id = $assetId""".as[String].list.head
        sqlu"""update text_property_value set value_fi='livi123' where id = $propertyValueId""".execute
        val dbResult = sql"""SELECT value_fi FROM text_property_value where id = $propertyValueId""".as[String].list
        dbResult.size should be(1)
        dbResult.head should be("livi123")
        val properties = List(
          SimpleProperty("tietojen_yllapitaja", List(PropertyValue("1"))),
          SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))))
        val position = Some(Position(374450, 6677250, 123l, None))
        RollbackMassTransitStopService.updateExistingById(assetId, position, properties.toSet, "user", _ => Unit)
        val massTransitStop = service.getById(assetId).get

        //The property yllapitajan_koodi should not have values
        val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
        liviIdentifierProperty.values.size should be(0)
      }
    }
  }

  test("Update last modified info") {
    runWithRollback {
      assetLock.synchronized {
        val geom = Point(374450, 6677250)
        val pos = Position(geom.x, geom.y, 131573L, Some(85))
        RollbackMassTransitStopService.updateExistingById(300000, Some(pos), Set.empty, "user", _ => Unit)
        val modifier = sql"""
            select a.modified_by from asset a
            where a.id = 300000
      """.as[String].firstOption
        modifier should be(Some("user"))
      }
    }
  }

  test("Update properties") {
    runWithRollback {
      assetLock.synchronized {
        val values = List(PropertyValue("New name"))
        val properties = Set(SimpleProperty("nimi_suomeksi", values))
        RollbackMassTransitStopService.updateExistingById(300000, None, properties, "user", _ => Unit)
        val modifier = sql"""
            select v.value_fi from text_property_value v
            join property p on v.property_id = p.id
            where v.asset_id = 300000 and p.public_id = 'nimi_suomeksi'
      """.as[String].firstOption
        modifier should be(Some("New name"))
      }
    }
  }

  test("Persist floating on update") {
    // This asset is actually supposed to be floating, but updateExisting shouldn't do a floating check
    runWithRollback {
      val position = Some(Position(60.0, 0.0, 123l, None))
      sql"""
            update asset a
            set floating = '0'
            where a.id = 300002
      """.asUpdate.execute
      RollbackMassTransitStopService.updateExistingById(300002, position, Set.empty, "user", _ => Unit)
      val floating = sql"""
            select a.floating from asset a
            where a.id = 300002
      """.as[Int].firstOption
      floating should be(Some(0))
    }
  }

  test("Send event to event bus in update") {
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
      val position = Some(Position(60.0, 0.0, 123l, None))
      service.updateExistingById(300002, position, Set.empty, "user", _ => Unit)
      verify(eventbus).publish(org.mockito.Matchers.eq("asset:saved"), any[EventBusMassTransitStop]())
    }
  }

  test("Assert user rights when updating a mass transit stop") {
    runWithRollback {
      val position = Some(Position(60.0, 0.0, 123l, None))
      an [Exception] should be thrownBy RollbackMassTransitStopService.updateExistingById(300002, position, Set.empty, "user", { municipalityCode => throw new Exception })
    }
  }

  test("Create new mass transit stop") {
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
      val values = List(PropertyValue("1"))
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("1"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))))
      val vvhRoadLink = VVHRoadlink(123l, 91, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = service.create(NewMassTransitStop(60.0, 0.0, 123l, 100, properties), "test", vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      val massTransitStop = service.getById(id).get
      massTransitStop.bearing should be(Some(100))
      massTransitStop.floating should be(false)
      massTransitStop.stopTypes should be(List(1))
      massTransitStop.validityPeriod should be(Some(MassTransitStopValidityPeriod.Current))

      //The property yllapitajan_koodi should not have values
      val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
      liviIdentifierProperty.values.size should be(0)

      verify(eventbus).publish(org.mockito.Matchers.eq("asset:saved"), any[EventBusMassTransitStop]())
    }
  }

  test("Create new virtual mass transit stop with Central ELY administration") {
    runWithRollback {
      val massTransitStopDao = new MassTransitStopDao
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("5"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))))
      val vvhRoadLink = VVHRoadlink(123l, 91, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = service.create(NewMassTransitStop(60.0, 0.0, 123l, 100, properties), "test", vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      val massTransitStop = service.getById(id).get
      massTransitStop.bearing should be(Some(100))
      massTransitStop.floating should be(false)
      massTransitStop.stopTypes should be(List(5))
      massTransitStop.validityPeriod should be(Some(MassTransitStopValidityPeriod.Current))

      //The property yllapitajan_koodi should not have values
      val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
      liviIdentifierProperty.values.size should be(0)

      verify(eventbus).publish(org.mockito.Matchers.eq("asset:saved"), any[EventBusMassTransitStop]())
    }
  }

  test("Create new mass transit stop with Central ELY administration") {
    runWithRollback {
      val massTransitStopDao = new MassTransitStopDao
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopServiceWithTierekisteri(eventbus, mockRoadLinkService)
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))),
        SimpleProperty("ensimmainen_voimassaolopaiva", List(PropertyValue("2013-01-01"))),
        SimpleProperty("viimeinen_voimassaolopaiva", List(PropertyValue(DateTime.now().plusDays(1).toString()))))
      val vvhRoadLink = VVHRoadlink(123l, 91, List(Point(0.0,0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = service.create(NewMassTransitStop(60.0, 0.0, 123l, 100, properties), "test", vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      val massTransitStop = service.getById(id).get
      massTransitStop.bearing should be(Some(100))
      massTransitStop.floating should be(false)
      massTransitStop.stopTypes should be(List(1))
      massTransitStop.validityPeriod should be(Some(MassTransitStopValidityPeriod.Current))

      //The property yllapitajan_koodi should be overridden with OTHJ + NATIONAL ID
      val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
      liviIdentifierProperty.values.head.propertyValue should be("OTHJ%d".format(massTransitStop.nationalId))

      verify(eventbus).publish(org.mockito.Matchers.eq("asset:saved"), any[EventBusMassTransitStop]())
    }
  }

  test("Create new mass transit stop with HSL administration and 'state' road link") {
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopServiceWithTierekisteri(eventbus, mockRoadLinkService)
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("3"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))),
        SimpleProperty("ensimmainen_voimassaolopaiva", List(PropertyValue("2013-01-01"))),
        SimpleProperty("viimeinen_voimassaolopaiva", List(PropertyValue(DateTime.now().plusDays(1).toString()))),
        SimpleProperty("linkin_hallinnollinen_luokka", List(PropertyValue("1"))))
      val vvhRoadLink = vvhRoadLinks.find(_.linkId==12333l).get
      val id = service.create(NewMassTransitStop(60.0, 0.0, 12333l, 100, properties), "test", vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      val massTransitStop = service.getById(id).get
      massTransitStop.bearing should be(Some(100))
      massTransitStop.floating should be(false)
      massTransitStop.stopTypes should be(List(1))
      massTransitStop.validityPeriod should be(Some(MassTransitStopValidityPeriod.Current))

      val administratorProperty = massTransitStop.propertyData.find(p => p.publicId == "tietojen_yllapitaja").get
      administratorProperty.values.head.propertyValue should be("3")

      val administrativeClassProperty = massTransitStop.propertyData.find(p => p.publicId == "linkin_hallinnollinen_luokka").get
      administrativeClassProperty.values.head.propertyValue should be("1")

      val liviIdentifierProperty = massTransitStop.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
      liviIdentifierProperty.values.head.propertyValue should be("OTHJ%d".format(massTransitStop.nationalId))

      val liviId = liviIdentifierProperty.values.head.propertyValue

      val captor: ArgumentCaptor[TierekisteriMassTransitStop] = ArgumentCaptor.forClass(classOf[TierekisteriMassTransitStop])
      verify(mockTierekisteriClient, Mockito.atLeastOnce).createMassTransitStop(captor.capture)
      val capturedStop = captor.getValue
      capturedStop.liviId should be (liviId)

      verify(eventbus).publish(org.mockito.Matchers.eq("asset:saved"), any[EventBusMassTransitStop]())
    }
  }

  test("Create new mass transit stop with HSL administration and 'state' road link and turn it into a municipality stop") {
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopServiceWithTierekisteri(eventbus, mockRoadLinkService)
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("3"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))),
        SimpleProperty("ensimmainen_voimassaolopaiva", List(PropertyValue("2013-01-01"))),
        SimpleProperty("viimeinen_voimassaolopaiva", List(PropertyValue("2027-01-01"))))
      val vvhRoadLink = VVHRoadlink(123l, 91, List(Point(0.0,0.0), Point(120.0, 0.0)), State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      when(mockRoadLinkService.getRoadLinkFromVVH(123l)).thenReturn(Some(vvhRoadLink).map(toRoadLink))
      val id = service.create(NewMassTransitStop(60.0, 0.0, 123l, 100, properties), "test", vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))

      val newProperties = Set(
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("1")))
      )

      service.updateExistingById(id, None, newProperties, "test2", Int => Unit)

      val massTransitStop = service.getById(id).get

      val administratorProperty = massTransitStop.propertyData.find(p => p.publicId == "tietojen_yllapitaja").get
      administratorProperty.values.head.propertyValue should be("1")

      val administrativeClassProperty = massTransitStop.propertyData.find(p => p.publicId == "linkin_hallinnollinen_luokka").get
      administrativeClassProperty.values.head.propertyValue should be("1")

      val captor: ArgumentCaptor[TierekisteriMassTransitStop] = ArgumentCaptor.forClass(classOf[TierekisteriMassTransitStop])
      val stringCaptor: ArgumentCaptor[Option[String]] = ArgumentCaptor.forClass(classOf[Option[String]])
      val stringCaptor2: ArgumentCaptor[Option[String]] = ArgumentCaptor.forClass(classOf[Option[String]])
      verify(mockTierekisteriClient, Mockito.atLeastOnce).updateMassTransitStop(captor.capture, stringCaptor.capture, stringCaptor2.capture)
      val trStop = captor.getValue
      stringCaptor.getValue shouldNot be (None)
      stringCaptor2.getValue shouldNot be (Some("test"))
      val liviId = stringCaptor.getValue.get

      liviId shouldNot be ("")
      trStop.operatingTo.isEmpty should be (false)
      trStop.operatingTo.get.getTime - new Date().getTime < 5*60*1000L should be (true)
    }
  }

  test("Project stop location on two-point geometry") {
    val linkGeometry: Seq[Point] = List(Point(0.0, 0.0), Point(1.0, 0.0))
    val location: Point = Point(0.5, 0.5)
    val mValue: Double = RollbackMassTransitStopService.calculateLinearReferenceFromPoint(location, linkGeometry)
    mValue should be(0.5)
  }

  test("Project stop location on three-point geometry") {
    val linkGeometry: Seq[Point] = List(Point(0.0, 0.0), Point(1.0, 0.0), Point(1.0, 0.5))
    val location: Point = Point(1.2, 0.25)
    val mValue: Double = RollbackMassTransitStopService.calculateLinearReferenceFromPoint(location, linkGeometry)
    mValue should be(1.25)
  }

  test("Project stop location to beginning of geometry if point lies behind geometry") {
    val linkGeometry: Seq[Point] = List(Point(0.0, 0.0), Point(1.0, 0.0))
    val location: Point = Point(-0.5, 0.0)
    val mValue: Double = RollbackMassTransitStopService.calculateLinearReferenceFromPoint(location, linkGeometry)
    mValue should be(0.0)
  }

  test("Project stop location to the end of geometry if point lies beyond geometry") {
    val linkGeometry: Seq[Point] = List(Point(0.0, 0.0), Point(1.0, 0.0))
    val location: Point = Point(1.5, 0.5)
    val mValue: Double = RollbackMassTransitStopService.calculateLinearReferenceFromPoint(location, linkGeometry)
    mValue should be(1.0)
  }

  test ("Convert PersistedMassTransitStop into TierekisteriMassTransitStop") {
    def massTransitStopTransformation(stop: PersistedMassTransitStop): (PersistedMassTransitStop, Option[FloatingReason]) = {
      (stop, None)
    }
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    runWithRollback {

      sqlu"""Insert into LRM_POSITION (ID,LANE_CODE,SIDE_CODE,START_MEASURE,END_MEASURE,MML_ID,LINK_ID,ADJUSTED_TIMESTAMP,MODIFIED_DATE)
    values ('5400000',null,'1','109','298,694',null,'1021227','0',to_timestamp('17.02.17 12:21:39,227710000','RR.MM.DD HH24:MI:SSXFF'))""".execute

      sqlu"""Insert into ROAD_ADDRESS (ID,ROAD_NUMBER,ROAD_PART_NUMBER,TRACK_CODE,DISCONTINUITY,START_ADDR_M,END_ADDR_M,LRM_POSITION_ID,START_DATE,END_DATE,CREATED_BY,VALID_FROM,CALIBRATION_POINTS,FLOATING,GEOMETRY,VALID_TO)
    values ('7000000','1','1','0','5','0','299','5400000',to_date('01.09.12','RR.MM.DD'),null,'tr',to_date('01.09.12','RR.MM.DD'),'2','0',MDSYS.SDO_GEOMETRY(4002,3067,NULL,MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),MDSYS.SDO_ORDINATE_ARRAY(385258.765,7300119.103,0,0,384984.756,7300237.964,0,299)),null)""".execute

      val assetId = 300006
      val stopOption = RollbackMassTransitStopService.fetchPointAssets((s:String) => s"""$s where a.id = $assetId""").headOption
      stopOption.isEmpty should be (false)
      val stop = stopOption.get
      val (address, roadSide) = geometryTransform.resolveAddressAndLocation(stop.mValue, stop.linkId, stop.validityDirection.get, Some(stop.municipalityCode))
      val trStop = TierekisteriBusStopMarshaller.toTierekisteriMassTransitStop(stop, address, Option(roadSide))
      roadSide should be (RoadSide.Left)
      trStop.nationalId should be (stop.nationalId)
      trStop.stopType should be (StopType.LongDistance)
      trStop.equipments.get(Equipment.Roof).get should be (Existence.Yes)
      trStop.equipments.filterNot( x => x._1 == Equipment.Roof).forall(_._2 == Existence.Unknown) should be (true)
      trStop.operatingFrom.isEmpty should be (false)
      trStop.operatingTo.isEmpty should be (false)
      val opFrom = stop.propertyData.find(_.publicId=="ensimmainen_voimassaolopaiva").flatMap(_.values.headOption.map(_.propertyValue))
      val opTo = stop.propertyData.find(_.publicId=="viimeinen_voimassaolopaiva").flatMap(_.values.headOption.map(_.propertyValue))
      opFrom shouldNot be (None)
      opTo shouldNot be (None)
      dateFormatter.format(trStop.operatingFrom.get) should be (opFrom.get)
      dateFormatter.format(trStop.operatingTo.get) should be (opTo.get)
    }
  }

  test ("Test date conversions in Marshaller") {
    def massTransitStopTransformation(stop: PersistedMassTransitStop): (PersistedMassTransitStop, Option[FloatingReason]) = {
      (stop, None)
    }
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    runWithRollback {

      sqlu"""Insert into LRM_POSITION (ID,LANE_CODE,SIDE_CODE,START_MEASURE,END_MEASURE,MML_ID,LINK_ID,ADJUSTED_TIMESTAMP,MODIFIED_DATE)
    values ('5400000',null,'1','109','298,694',null,'1021227','0',to_timestamp('17.02.17 12:21:39,227710000','RR.MM.DD HH24:MI:SSXFF'))""".execute

      sqlu"""Insert into ROAD_ADDRESS (ID,ROAD_NUMBER,ROAD_PART_NUMBER,TRACK_CODE,DISCONTINUITY,START_ADDR_M,END_ADDR_M,LRM_POSITION_ID,START_DATE,END_DATE,CREATED_BY,VALID_FROM,CALIBRATION_POINTS,FLOATING,GEOMETRY,VALID_TO)
    values ('7000000','1','1','0','5','0','299','5400000',to_date('01.09.12','RR.MM.DD'),null,'tr',to_date('01.09.12','RR.MM.DD'),'2','0',MDSYS.SDO_GEOMETRY(4002,3067,NULL,MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),MDSYS.SDO_ORDINATE_ARRAY(385258.765,7300119.103,0,0,384984.756,7300237.964,0,299)),null)""".execute

      val assetId = 300006
      val stopOption = RollbackMassTransitStopService.fetchPointAssets((s:String) => s"""$s where a.id = $assetId""").headOption
      stopOption.isEmpty should be (false)
      val stop = stopOption.get
      val (address, roadSide) = geometryTransform.resolveAddressAndLocation(stop.mValue, stop.linkId, stop.validityDirection.get, Some(stop.municipalityCode))
      val expireDate = new Date(10487450L)
      val trStop = TierekisteriBusStopMarshaller.toTierekisteriMassTransitStop(stop, address, Option(roadSide), Some(expireDate))
      trStop.operatingTo.isEmpty should be (false)
      trStop.operatingTo.get should be (expireDate)
      val trStopNoExpireDate = TierekisteriBusStopMarshaller.toTierekisteriMassTransitStop(stop, address, Option(roadSide), None)
      trStopNoExpireDate.operatingTo.isEmpty should be (false)
      trStopNoExpireDate.operatingTo.get shouldNot be (expireDate)
    }
  }

  test("Update existing masstransitstop if the new distance is greater than 50 meters"){
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))),
        SimpleProperty("vaikutussuunta", List(PropertyValue("2"))))
      val linkId = 123l
      val municipalityCode = 91
      val geometry = Seq(Point(0.0,0.0), Point(120.0, 0.0))

      when(mockVVHClient.fetchByLinkId(linkId))
        .thenReturn(Some(VVHRoadlink(linkId, municipalityCode, geometry, Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)))

      val oldAssetId = service.create(NewMassTransitStop(0, 0, linkId, 0, properties), "test", geometry, municipalityCode, Some(Municipality))
      val oldAsset = sql"""select id, municipality_code, valid_from, valid_to from asset where id = $oldAssetId""".as[(Long, Int, String, String)].firstOption
      oldAsset should be (Some(oldAssetId, municipalityCode, null, null))

      val updatedAssetId = service.updateExistingById(oldAssetId, Some(Position(0, 51, linkId, Some(0))), Set(), "test",  _ => Unit).id

      val newAsset = sql"""select id, municipality_code, valid_from, valid_to from asset where id = $updatedAssetId""".as[(Long, Int, String, String)].firstOption
      newAsset should be (Some(updatedAssetId, municipalityCode, null, null))

      val expired = sql"""select case when a.valid_to <= sysdate then 1 else 0 end as expired from asset a where id = $oldAssetId""".as[(Boolean)].firstOption
      expired should be(Some(true))
    }
  }

  test("Should not copy existing masstransitstop if the new distance is less or equal than 50 meters"){
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
      val properties = List(
        SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
        SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
        SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))))
      val linkId = 123l
      val municipalityCode = 91
      val geometry = Seq(Point(0.0,0.0), Point(120.0, 0.0))

      when(mockVVHClient.fetchByLinkId(linkId))
        .thenReturn(Some(VVHRoadlink(linkId, municipalityCode, geometry, Municipality, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)))

      val assetId = service.create(NewMassTransitStop(0, 0, linkId, 0, properties), "test", geometry , municipalityCode, Some(Municipality))
      val asset = sql"""select id, municipality_code, valid_from, valid_to from asset where id = $assetId""".as[(Long, Int, String, String)].firstOption
      asset should be (Some(assetId, municipalityCode, null, null))

      val updatedAssetId = service.updateExistingById(assetId, Some(Position(0, 50, linkId, Some(0))), Set(), "test",  _ => Unit).id
      updatedAssetId should be(assetId)

      val expired = sql"""select case when a.valid_to <= sysdate then 1 else 0 end as expired from asset a where id = $assetId""".as[(Boolean)].firstOption
      expired should be(Some(false))
    }
  }

  test("delete a TR kept mass transit stop") {
    runWithRollback {
      val eventbus = MockitoSugar.mock[DigiroadEventBus]
      val service = new TestMassTransitStopServiceWithTierekisteri(eventbus, mockRoadLinkService)
      when(mockTierekisteriClient.isTREnabled).thenReturn(true)
      val (stop, showStatusCode) = service.getMassTransitStopByNationalIdWithTRWarnings(85755, Int => Unit)
      service.deleteAllMassTransitStopData(stop.head.id)
      verify(mockTierekisteriClient).deleteMassTransitStop("OTHJ85755")
      service.getMassTransitStopByNationalIdWithTRWarnings(85755, Int => Unit)._1.isEmpty should be(true)
      service.getMassTransitStopByNationalIdWithTRWarnings(85755, Int => Unit)._2 should be (false)
    }
  }

  test("delete fails if a TR kept mass transit stop is not found") {
    val eventbus = MockitoSugar.mock[DigiroadEventBus]
    val service = new TestMassTransitStopServiceWithDynTransaction(eventbus, mockRoadLinkService)
    val (stop, showStatusCode) = service.getMassTransitStopByNationalIdWithTRWarnings(85755, Int => Unit)
    when(mockTierekisteriClient.deleteMassTransitStop(any[String])).thenThrow(new TierekisteriClientException("foo"))
    intercept[TierekisteriClientException] {
      when(mockTierekisteriClient.isTREnabled).thenReturn(true)
      service.deleteAllMassTransitStopData(stop.head.id)
    }
    service.getMassTransitStopByNationalIdWithTRWarnings(85755, Int => Unit)._1.isEmpty should be(false)
  }

  test("Updating the mass transit stop from others -> ELY should create a stop in TR") {
    runWithRollback {
      reset(mockTierekisteriClient)
      val vvhRoadLink = VVHRoadlink(11, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = RollbackMassTransitStopService.create(NewMassTransitStop(5.0, 0.0, 1l, 2,
        Seq(
          SimpleProperty("tietojen_yllapitaja", Seq(PropertyValue("1"))),
          SimpleProperty("pysakin_tyyppi", Seq(PropertyValue("2"))),
          SimpleProperty("vaikutussuunta", Seq(PropertyValue("2")))
        )), "masstransitstopservice_spec",
        vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      when(mockGeometryTransform.resolveAddressAndLocation(any[Double], any[Long], any[Int], any[Option[Int]], any[Option[Int]]
        )).thenReturn((new RoadAddress(Some("235"), 110, 10, Track.Combined, 108, None), RoadSide.Right))
      val service = RollbackMassTransitStopServiceWithTierekisteri
      val stop = service.getById(id).get
      val props = stop.propertyData
      val admin = props.find(_.publicId == "tietojen_yllapitaja").get
      val newAdmin = admin.copy(values = List(PropertyValue("2")))
      val types = props.find(_.publicId == "pysakin_tyyppi").get
      val newTypes = types.copy(values = List(PropertyValue("2"), PropertyValue("3")))
      admin.values.exists(_.propertyValue == "1") should be (true)
      types.values.exists(_.propertyValue == "2") should be (true)
      types.values.exists(_.propertyValue == "3") should be (false)
      val newStop = stop.copy(stopTypes = Seq(2, 3),
        propertyData = props.filterNot(_.publicId == "tietojen_yllapitaja").filterNot(_.publicId == "pysakin_tyyppi") ++
          Seq(newAdmin, newTypes))
      val newProps = newStop.propertyData.map(prop => SimpleProperty(prop.publicId, prop.values)).toSet
      service.updateExistingById(stop.id, None, newProps, "seppo", { (Int) => Unit })
      verify(mockTierekisteriClient, times(1)).createMassTransitStop(any[TierekisteriMassTransitStop])
      verify(mockTierekisteriClient, times(0)).updateMassTransitStop(any[TierekisteriMassTransitStop], any[Option[String]], any[Option[String]])
    }
  }

  test("Updating the mass transit stop from others -> HSL should create a stop in TR") {
    runWithRollback {
      reset(mockTierekisteriClient)
      val vvhRoadLink = VVHRoadlink(11, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = RollbackMassTransitStopService.create(NewMassTransitStop(5.0, 0.0, 1l, 2,
        Seq(
          SimpleProperty("tietojen_yllapitaja", Seq(PropertyValue("1"))),
          SimpleProperty("pysakin_tyyppi", Seq(PropertyValue("2"))),
          SimpleProperty("vaikutussuunta", Seq(PropertyValue("2")))
        )), "masstransitstopservice_spec",
        vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      when(mockGeometryTransform.resolveAddressAndLocation(any[Double], any[Long], any[Int], any[Option[Int]], any[Option[Int]]
      )).thenReturn((new RoadAddress(Some("235"), 110, 10, Track.Combined, 108, None), RoadSide.Right))
      val service = RollbackMassTransitStopServiceWithTierekisteri
      val stop = service.getById(id).get
      val props = stop.propertyData
      val admin = props.find(_.publicId == "tietojen_yllapitaja").get
      val newAdmin = admin.copy(values = List(PropertyValue("3")))
      val types = props.find(_.publicId == "pysakin_tyyppi").get
      val newTypes = types.copy(values = List(PropertyValue("2"), PropertyValue("3")))
      admin.values.exists(_.propertyValue == "1") should be (true)
      types.values.exists(_.propertyValue == "2") should be (true)
      types.values.exists(_.propertyValue == "3") should be (false)
      val newStop = stop.copy(stopTypes = Seq(2, 3),
        propertyData = props.filterNot(_.publicId == "tietojen_yllapitaja").filterNot(_.publicId == "pysakin_tyyppi") ++
          Seq(newAdmin, newTypes))
      val newProps = newStop.propertyData.map(prop => SimpleProperty(prop.publicId, prop.values)).toSet
      service.updateExistingById(stop.id, None, newProps, "seppo", { (Int) => Unit })
      verify(mockTierekisteriClient, times(1)).createMassTransitStop(any[TierekisteriMassTransitStop])
      verify(mockTierekisteriClient, times(0)).updateMassTransitStop(any[TierekisteriMassTransitStop], any[Option[String]], any[Option[String]])
    }
  }

  test("Saving a mass transit stop should not create a stop in TR") {
    runWithRollback {
      reset(mockTierekisteriClient)
      val vvhRoadLink = VVHRoadlink(11, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = RollbackMassTransitStopService.create(NewMassTransitStop(5.0, 0.0, 1l, 2,
        Seq(
          SimpleProperty("tietojen_yllapitaja", Seq(PropertyValue("1"))),
          SimpleProperty("pysakin_tyyppi", Seq(PropertyValue("2"))),
          SimpleProperty("vaikutussuunta", Seq(PropertyValue("2")))
        )), "masstransitstopservice_spec",
        vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      verify(mockTierekisteriClient, times(0)).updateMassTransitStop(any[TierekisteriMassTransitStop], any[Option[String]], any[Option[String]])
      verify(mockTierekisteriClient, times(0)).createMassTransitStop(any[TierekisteriMassTransitStop])
    }
  }
  test("Updating the mass transit stop from ELY -> others should expire a stop in TR") {
    runWithRollback {
      reset(mockTierekisteriClient)
      val vvhRoadLink = VVHRoadlink(11, 235, List(Point(0.0,0.0), Point(120.0, 0.0)), State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers)
      val id = RollbackMassTransitStopService.create(NewMassTransitStop(5.0, 0.0, 1l, 2,
        Seq(
          SimpleProperty("tietojen_yllapitaja", Seq(PropertyValue("2"))),
          SimpleProperty("pysakin_tyyppi", Seq(PropertyValue("2"), PropertyValue("3"))),
          SimpleProperty("vaikutussuunta", Seq(PropertyValue("2")))
        )), "masstransitstopservice_spec",
        vvhRoadLink.geometry, vvhRoadLink.municipalityCode, Some(vvhRoadLink.administrativeClass))
      when(mockGeometryTransform.resolveAddressAndLocation(any[Double], any[Long], any[Int], any[Option[Int]], any[Option[Int]]
        )).thenReturn((new RoadAddress(Some("235"), 110, 10, Track.Combined, 108, None), RoadSide.Right))
      val service = RollbackMassTransitStopServiceWithTierekisteri
      val stop = service.getById(id).get
      val props = stop.propertyData
      val admin = props.find(_.publicId == "tietojen_yllapitaja").get.copy(values = List(PropertyValue("1")))
      val newStop = stop.copy(stopTypes = Seq(2, 3), propertyData = props.filterNot(_.publicId == "tietojen_yllapitaja") ++ Seq(admin))
      val newProps = newStop.propertyData.map(prop => SimpleProperty(prop.publicId, prop.values)).toSet
      service.updateExistingById(stop.id, None, newProps, "seppo", { (Int) => Unit })
      verify(mockTierekisteriClient, times(1)).createMassTransitStop(any[TierekisteriMassTransitStop])
      verify(mockTierekisteriClient, times(1)).updateMassTransitStop(any[TierekisteriMassTransitStop], any[Option[String]], any[Option[String]])
    }
  }

  test ("Get enumerated property values") {
    runWithRollback {
      val propertyValues = RollbackMassTransitStopService.massTransitStopEnumeratedPropertyValues
      propertyValues.nonEmpty should be (true)
      propertyValues.forall(x => x._2.nonEmpty) should be (true)
      propertyValues.forall(x => x._1 != "") should be (true)
    }
  }

  test("Stop floats if a State road has got changed to a road owned by municipality"){
    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)

    val massTransitStopDao = new MassTransitStopDao
    runWithRollback{
      val assetId = 300006
      val boundingBox = BoundingRectangle(Point(370000,6077000), Point(374800,6677600))
      //Set administration class of the asset with State value
      RollbackMassTransitStopService.updateAdministrativeClassValue(assetId, State)
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBox)
      stops.find(_.id == assetId).map(_.floating) should be(Some(true))
      massTransitStopDao.getAssetFloatingReason(assetId) should be(Some(FloatingReason.RoadOwnerChanged))
    }
  }

  test("Stop floats if a State road has got changed to a road owned to a private road"){
    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)

    val massTransitStopDao = new MassTransitStopDao
    runWithRollback{
      val assetId = 300012
      val boundingBox = BoundingRectangle(Point(370000,6077000), Point(374800,6677600))
      //Set administration class of the asset with State value
      RollbackMassTransitStopService.updateAdministrativeClassValue(assetId, State)
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBox)
      stops.find(_.id == assetId).map(_.floating) should be(Some(true))
      massTransitStopDao.getAssetFloatingReason(assetId) should be(Some(FloatingReason.RoadOwnerChanged))
    }
  }

  test("Stop floats if a Municipality road has got changed to a road owned by state"){
    val vvhRoadLinks = List(
      VVHRoadlink(1021227, 90, Nil, State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers))

    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)
    when(mockRoadLinkService.getRoadLinksFromVVH(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks.map(toRoadLink))

    val massTransitStopDao = new MassTransitStopDao
    runWithRollback{
      val assetId = 300006
      val boundingBox = BoundingRectangle(Point(370000,6077000), Point(374800,6677600))
      //Set administration class of the asset with State value
      RollbackMassTransitStopService.updateAdministrativeClassValue(assetId, Municipality)
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBox)
      stops.find(_.id == assetId).map(_.floating) should be(Some(true))
      massTransitStopDao.getAssetFloatingReason(assetId) should be(Some(FloatingReason.RoadOwnerChanged))
    }
  }

  test("Stop floats if a Private road has got changed to a road owned by state"){
    val vvhRoadLinks = List(
      VVHRoadlink(1021227, 90, Nil, State, TrafficDirection.UnknownDirection, FeatureClass.AllOthers))

    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)
    when(mockRoadLinkService.getRoadLinksFromVVH(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks.map(toRoadLink))

    val massTransitStopDao = new MassTransitStopDao
    runWithRollback{
      val assetId = 300006
      val boundingBox = BoundingRectangle(Point(370000,6077000), Point(374800,6677600))
      //Set administration class of the asset with State value
      RollbackMassTransitStopService.updateAdministrativeClassValue(assetId, Private)
      val stops = RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBox)
      stops.find(_.id == assetId).map(_.floating) should be(Some(true))
      massTransitStopDao.getAssetFloatingReason(assetId) should be(Some(FloatingReason.RoadOwnerChanged))
    }
  }

  test("Stops working list shouldn't have floating assets with floating reason RoadOwnerChanged if user is not operator"){
    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)

    runWithRollback {
      val assetId = 300012
      val boundingBox = BoundingRectangle(Point(370000,6077000), Point(374800,6677600))
      //Set administration class of the asset with State value
      RollbackMassTransitStopService.updateAdministrativeClassValue(assetId, State)
      //GetBoundingBox will set assets  to floating
      RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBox)
      val workingList = RollbackMassTransitStopService.getFloatingAssetsWithReason(Some(Set(235)), Some(false))
      //Get all external ids from the working list
      val externalIds = workingList.map(m => m._2.map(a => a._2).flatten).flatten

      //Should not find any external id of the asset with administration class changed
      externalIds.foreach{ externalId =>
        externalId.get("id") should not be (Some(8))
      }
    }
  }

  test("Stops working list should have all floating assets if user is operator"){
    when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)

    runWithRollback {
      val assetId = 300012
      val boundingBox = BoundingRectangle(Point(370000,6077000), Point(374800,6677600))
      //Set administration class of the asset with State value
      RollbackMassTransitStopService.updateAdministrativeClassValue(assetId, State)
      //GetBoundingBox will set assets  to floating
      RollbackMassTransitStopService.getByBoundingBox(userWithKauniainenAuthorization, boundingBox)
      val workingList = RollbackMassTransitStopService.getFloatingAssetsWithReason(Some(Set(235)), Some(true))
      //Get all external ids from the working list
      val externalIds = workingList.map(m => m._2.map(a => a._2).flatten).flatten

      //Should have the external id of the asset with administration class changed
      externalIds.map(_.get("id")) should contain (Some(8))
    }
  }

  test("getByMunicipality gets Tierekisteri Equipment") {
    val combinations: List[(Existence, String)] = List(Existence.No, Existence.Yes, Existence.Unknown).zip(List("1", "2", "99"))
    combinations.foreach { case (e, v) =>
      reset(mockTierekisteriClient, mockRoadLinkService)
      when(mockTierekisteriClient.fetchMassTransitStop(any[String])).thenReturn(Some(
        TierekisteriMassTransitStop(2, "2", RoadAddress(None, 1, 1, Track.Combined, 1, None), TRRoadSide.Unknown, StopType.Combined,
          false, equipments = Equipment.values.map(equip => equip -> e).toMap, None, None, None, "KX12356", None, None, None, new Date))
      )
      when(mockRoadLinkService.getRoadLinksFromVVH(any[Int])).thenReturn(vvhRoadLinks.map(toRoadLink))

      runWithRollback {
        val stops = RollbackMassTransitStopServiceWithTierekisteri.getByMunicipality(235)
        val (trStops, _) = stops.partition(s => MassTransitStopOperations.isStoredInTierekisteri(Some(s)))
        val equipmentPublicIds = Equipment.values.filter(_.isMaster).map(_.publicId)
        // All should be unknown as set in the TRClientMock
        val equipments = trStops.map(t => t.propertyData.filter(p => equipmentPublicIds.contains(p.publicId)))
        equipments.forall(_.forall(p => p.values.nonEmpty && p.values.head.propertyValue == v)) should be(true)
      }
      verify(mockRoadLinkService, times(1)).getRoadLinksFromVVH(any[Int])
      verify(mockTierekisteriClient, Mockito.atLeast(1)).fetchMassTransitStop(any[String])
    }
  }

  test("Updating an existing stop should not create a new Livi ID") {
    val equipments = Map[Equipment, Existence](
      Equipment.BikeStand -> Existence.Yes,
      Equipment.CarParkForTakingPassengers -> Existence.Unknown,
      Equipment.ElectronicTimetables -> Existence.Yes,
      Equipment.RaisedBusStop -> Existence.No,
      Equipment.Lighting -> Existence.Unknown,
      Equipment.Roof -> Existence.Yes,
      Equipment.Seat -> Existence.Unknown,
      Equipment.Timetable -> Existence.No,
      Equipment.TrashBin -> Existence.Yes,
      Equipment.RoofMaintainedByAdvertiser -> Existence.Yes
    )
    runWithRollback {
      val rad = RoadAddress(Some("235"), 110, 10, Track.Combined, 108, None)
      when(mockGeometryTransform.resolveAddressAndLocation(any[Double], any[Long], any[Int], any[Option[Int]], any[Option[Int]]
        )).thenReturn((rad, RoadSide.Right))
      when(mockVVHClient.queryByMunicipalitesAndBounds(any[BoundingRectangle], any[Set[Int]])).thenReturn(vvhRoadLinks)
      val trStop = TierekisteriMassTransitStop(85755, "livi114873", rad, TRRoadSide.Unknown, StopType.Unknown, false,
        equipments, None, Option("TierekisteriFi"), Option("TierekisteriSe"), "test", Option(new Date), Option(new Date), Option(new Date), new Date(2016, 8, 1))

      sqlu"""update asset set floating=1 where id = 300008""".execute
      sqlu"""update text_property_value set value_fi='livi114873' where asset_id = 300008 and value_fi = 'OTHJ85755'""".execute
      when(mockTierekisteriClient.fetchMassTransitStop("livi114873")).thenReturn(Some(trStop))
      val (stopOpt, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(85755, _ => Unit)
      val stop = stopOpt.get
      RollbackMassTransitStopServiceWithTierekisteri.updateExistingById(stop.id,
        None, stop.propertyData.map(p =>
          if (p.publicId != MassTransitStopOperations.LiViIdentifierPublicId)
            SimpleProperty(p.publicId, p.values)
        else
            SimpleProperty(p.publicId, Seq(PropertyValue("1")))
        ).toSet, "pekka", Int => Unit)
      val captor: ArgumentCaptor[TierekisteriMassTransitStop] = ArgumentCaptor.forClass(classOf[TierekisteriMassTransitStop])
      verify(mockTierekisteriClient, Mockito.atLeastOnce).updateMassTransitStop(captor.capture, any[Option[String]], any[Option[String]])
      val capturedStop = captor.getValue
      capturedStop.liviId should be ("livi114873")
      val dbResult = sql"""SELECT value_fi FROM text_property_value where value_fi='livi114873' and asset_id = 300008""".as[String].list
      dbResult.size should be (1)
      dbResult.head should be ("livi114873")
    }
  }

  test("Should rollback bus stop if tierekisteri throw exception") {
    val assetId = 300000
    val geom = Point(374550, 6677350)
    val pos = Position(geom.x, geom.y, 131573L, Some(85))
    val properties = List(
      SimpleProperty("pysakin_tyyppi", List(PropertyValue("1"))),
      SimpleProperty("tietojen_yllapitaja", List(PropertyValue("2"))),
      SimpleProperty("yllapitajan_koodi", List(PropertyValue("livi"))))

    val service = new TestMassTransitStopServiceWithDynTransaction(new DummyEventBus, mockRoadLinkService)
    when(mockTierekisteriClient.isTREnabled).thenReturn(true)
    when(mockTierekisteriClient.updateMassTransitStop(any[TierekisteriMassTransitStop], any[Option[String]], any[Option[String]])).thenThrow(new TierekisteriClientException("TR-test exception"))
    when(mockGeometryTransform.resolveAddressAndLocation(any[Double], any[Long], any[Int], any[Option[Int]], any[Option[Int]])).thenReturn(
      (RoadAddress(Option("235"), 1, 1, Track.Combined, 0, None), RoadSide.Left)
    )

    intercept[TierekisteriClientException] {
      service.updateExistingById(300000, Some(pos), properties.toSet, "user", _ => Unit)
    }

    val asset = service.getById(300000).get
    asset.validityPeriod should be(Some(MassTransitStopValidityPeriod.Current))
  }

  test("Should not crash TR stop without livi id"){
    runWithRollback {
      assetLock.synchronized {
        val eventbus = MockitoSugar.mock[DigiroadEventBus]
        val service = new TestMassTransitStopService(eventbus, mockRoadLinkService)
        when(mockTierekisteriClient.isTREnabled).thenReturn(true)
        val assetId = 300000
        val propertyValueId = sql"""SELECT id FROM text_property_value where value_fi='OTHJ1' and asset_id = $assetId""".as[String].list.head
        sqlu"""delete from text_property_value where id = $propertyValueId""".execute
        val dbResult = sql"""SELECT value_fi FROM text_property_value where id = $propertyValueId""".as[String].list
        dbResult.size should be(0)
        val (stop, showStatusCode) = RollbackMassTransitStopServiceWithTierekisteri.getMassTransitStopByNationalIdWithTRWarnings(1, _ => Unit)
        stop.isDefined should be(true)
        val liviIdentifierProperty = stop.get.propertyData.find(p => p.publicId == "yllapitajan_koodi").get
        liviIdentifierProperty.values.isEmpty should be(true)
        showStatusCode should be(true)
      }
    }
  }

}
