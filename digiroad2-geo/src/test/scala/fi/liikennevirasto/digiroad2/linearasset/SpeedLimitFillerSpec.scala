package fi.liikennevirasto.digiroad2.linearasset

import fi.liikennevirasto.digiroad2.asset.TrafficDirection.TowardsDigitizing
import org.joda.time.DateTime
import fi.liikennevirasto.digiroad2.linearasset.LinearAssetFiller.{SideCodeAdjustment, MValueAdjustment, ChangeSet}
import fi.liikennevirasto.digiroad2.linearasset.SpeedLimitFiller.Projection
import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.asset._
import org.scalatest._

class SpeedLimitFillerSpec extends FunSuite with Matchers {
  private def roadLink(linkId: Long, geometry: Seq[Point], administrativeClass: AdministrativeClass = Unknown): RoadLink = {
    val municipalityCode = "MUNICIPALITYCODE" -> BigInt(235)
    RoadLink(
      linkId, geometry, GeometryUtils.geometryLength(geometry), administrativeClass, 1,
      TrafficDirection.BothDirections, Motorway, None, None, Map(municipalityCode))
  }

  private def oneWayRoadLink(linkId: Int, geometry: Seq[Point], trafficDirection: TrafficDirection) = {
    roadLink(linkId, geometry).copy(trafficDirection = trafficDirection)
  }

  test("drop segment outside of link geometry") {
    val topology = Seq(
      roadLink(2, Seq(Point(1.0, 0.0), Point(2.0, 0.0))))
    val speedLimits = Map(2l -> Seq(
      SpeedLimit(1, 2, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(80)), Seq(Point(1.0, 0.0), Point(2.0, 0.0)), 2.15, 2.35, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    changeSet.droppedAssetIds should be(Set(1))
  }

  test("adjust speed limit to cover whole link when its the only speed limit to refer to the link") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0))))
    val speedLimits = Map(1l -> Seq(
      SpeedLimit(1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)), Seq(Point(2.0, 0.0), Point(9.0, 0.0)), 2.0, 9.0, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology.length should be(1)
    filledTopology.head.geometry should be(Seq(Point(0.0, 0.0), Point(10.0, 0.0)))
    filledTopology.head.startMeasure should be(0.0)
    filledTopology.head.endMeasure should be(10.0)
    changeSet should be(ChangeSet(Set.empty, Seq(MValueAdjustment(1, 1, 0, 10.0)), Nil, Set.empty))
  }

  test("adjust one way speed limits to cover whole link when there are no multiple speed limits on one side of the link") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0))))
    val speedLimits = Map(
      1l -> Seq(
        SpeedLimit(
          1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
          Seq(Point(2.0, 0.0), Point(9.0, 0.0)), 2.0, 9.0, None, None, None, None, 0, None),
        SpeedLimit(
          2, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
          Seq(Point(2.0, 0.0), Point(9.0, 0.0)), 2.0, 9.0, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 2
    filledTopology.map(_.geometry) should be(Seq(
      Seq(Point(0.0, 0.0), Point(10.0, 0.0)),
      Seq(Point(0.0, 0.0), Point(10.0, 0.0))))
    filledTopology.map(_.startMeasure) should be(Seq(0.0, 0.0))
    filledTopology.map(_.endMeasure) should be(Seq(10.0, 10.0))
    changeSet.adjustedMValues should have size 2
    changeSet.adjustedMValues should be(Seq(MValueAdjustment(1, 1, 0, 10.0), MValueAdjustment(2, 1, 0, 10.0)))
  }

  test("cap speed limit to road link geometry") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(100.0, 0.0))))
    val speedLimits = Map(1l -> Seq(
      SpeedLimit(1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)), Seq(Point(0.0, 0.0), Point(90.0, 0.0)), 0.0, 90.0, None, None, None, None, 0, None),
      SpeedLimit(2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)), Seq(Point(90.0, 0.0), Point(110.0, 0.0)), 90.0, 110.0, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology.length should be(2)

    filledTopology.find(_.id == 2).map(_.geometry) should be(Some(Seq(Point(90.0, 0.0), Point(100.0, 0.0))))
    filledTopology.find(_.id == 2).map(_.endMeasure) should be(Some(100.0))

    changeSet.adjustedMValues should be(Seq(MValueAdjustment(2, 1, 90.0, 100.0)))
  }

  test("drop short speed limit") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(100.00, 0.0))))
    val speedLimits = Map(
      1l -> Seq(SpeedLimit(1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)), Seq(Point(0.0, 0.0), Point(0.04, 0.0)), 0.0, 0.04, None, None, None, None, 0, None),
        SpeedLimit(2, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)), Seq(Point(0.04, 0.0), Point(100.0, 0.0)), 0.04, 100.0, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 1
    changeSet.droppedAssetIds should be(Set(1))
  }

  test("do not drop short speed limit if it fills the road length") {
  val topology = Seq(
    roadLink(1, Seq(Point(0.0, 0.0), Point(0.4, 0.0))))
  val speedLimits = Map(
    1l -> Seq(SpeedLimit(1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)), Seq(Point(0.0, 0.0), Point(0.4, 0.0)), 0.0, 0.4, None, None, None, None, 0, None)))
  val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
  filledTopology should have size 1
  changeSet.droppedAssetIds should be(Set())
}

test("should not drop adjusted short speed limit") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(1.0, 0.0))))
    val speedLimits = Map(
      1l -> Seq(SpeedLimit(1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)), Seq(Point(0.0, 0.0), Point(0.4, 0.0)), 0.0, 0.4, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 1
    filledTopology.map(_.id) should be(Seq(1))
    changeSet.droppedAssetIds shouldBe empty
  }

  test("adjust side code of a speed limit") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(1.0, 0.0))))
    val speedLimits = Map(
      1l -> Seq(SpeedLimit(1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)), Seq(Point(0.0, 0.0), Point(1.0, 0.0)), 0.0, 1.0, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 1
    filledTopology.map(_.sideCode) should be(Seq(SideCode.BothDirections))
    changeSet.adjustedSideCodes should have size 1
    changeSet.adjustedSideCodes.head should be(SideCodeAdjustment(1, SideCode.BothDirections))
  }

  test("adjust one-way speed limits on one-way road link into two-way speed limits") {
    val topology = Seq(
      oneWayRoadLink(1, Seq(Point(0.0, 0.0), Point(2.0, 0.0)), TrafficDirection.TowardsDigitizing))
    val speedLimits = Map(
      1l -> Seq(SpeedLimit(1, 1, SideCode.TowardsDigitizing, TrafficDirection.TowardsDigitizing, Some(NumericValue(40)), Seq(Point(0.0, 0.0), Point(1.0, 0.0)), 0.0, 1.0, None, None, None, None, 0, None),
        SpeedLimit(2, 1, SideCode.TowardsDigitizing, TrafficDirection.TowardsDigitizing, Some(NumericValue(50)), Seq(Point(1.0, 0.0), Point(2.0, 0.0)), 1.0, 2.0, None, None, None, None, 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 2
    filledTopology.map(_.sideCode) should be(Seq(SideCode.BothDirections, SideCode.BothDirections))
    changeSet.adjustedSideCodes should have size 2
    changeSet.adjustedSideCodes.toSet should be(Set(SideCodeAdjustment(1, SideCode.BothDirections), SideCodeAdjustment(2, SideCode.BothDirections)))
  }

  test("merge speed limits with same value on shared road link") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(1.0, 0.0))))
    val speedLimits = Map(
      1l -> Seq(
        SpeedLimit(
          1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
          Seq(Point(0.0, 0.0), Point(0.2, 0.0)), 0.0, 0.2, None, None, Some("one"), Some(DateTime.now().minus(1000)), 0, None),
        SpeedLimit(
          2, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
          Seq(Point(0.2, 0.0), Point(0.55, 0.0)), 0.2, 0.55, Some("one else"), Some(DateTime.now().minus(500)),
          Some("one"), Some(DateTime.now().minus(1000)), 0, None),
        SpeedLimit(
          3, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)),
          Seq(Point(0.55, 0.0), Point(1.0, 0.0)), 0.55, 1.0, Some("random guy"), Some(DateTime.now().minus(450)),
          Some("one"), Some(DateTime.now().minus(1100)), 0, None)))
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 1
    filledTopology.map(_.sideCode) should be(Seq(SideCode.BothDirections))
    filledTopology.map(_.value) should be(Seq(Some(NumericValue(40))))
    filledTopology.map(_.modifiedBy) should be (Seq(Some("random guy"))) // latest modification should show
    changeSet.adjustedMValues should be(Seq(MValueAdjustment(3, 1, 0.0, 1.0)))
    changeSet.adjustedSideCodes should be(List())
    changeSet.droppedAssetIds should be(Set(1, 2))
  }

  test("create unknown speed limit on empty segments") {
    val topology = Seq(
      roadLink(1, Seq(Point(0.0, 0.0), Point(100.0, 0.0)), State))
    val speedLimits = Map.empty[Long, Seq[SpeedLimit]]
    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(topology, speedLimits)
    filledTopology should have size 1
    filledTopology.map(_.sideCode) should be(Seq(SideCode.BothDirections))
    filledTopology.map(_.value) should be(Seq(None))
    filledTopology.map(_.id) should be(Seq(0))
  }

  test("project speed limits to new geometry, case 1 - single speed, both directions") {
    val oldRoadLink = roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0)))
    val newLink1 = roadLink(1, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val newLink2 = roadLink(2, Seq(Point(0.0, 0.0), Point(4.0, 0.0)))
    val newLink3 = roadLink(3, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val linkmap = Map(1L -> newLink1, 2L -> newLink2, 3L -> newLink3)
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(0.0, 0.0), Point(10.0, 0.0)), 0.0, 10.0, None, None, None, None, 0, None))
    val changes = Seq(ChangeInfo(Some(1l), Some(1l), 2l, 5, Some(0.0), Some(3.0), Some(0.0), Some(3.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(2l), 22, 6, Some(3.0), Some(7.0), Some(0.0), Some(4.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(3l), 23, 6, Some(7.0), Some(10.0), Some(3.0), Some(0.0), Some(1440000))
    )
    val output = changes map { change =>
      SpeedLimitFiller.projectSpeedLimit(speedLimit.head, linkmap.get(change.newId.get).get,
      Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get)) }
    output.length should be(3)
    output.head.trafficDirection should be (TrafficDirection.BothDirections)
    output.head.startMeasure should be(0.0)
    output.head.endMeasure should be(3.0)
  }

  test("project speed limits to new geometry, case 2 - different speeds to different directions") {
    val oldRoadLink = roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0)))
    val newLink1 = roadLink(1, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val newLink2 = roadLink(2, Seq(Point(0.0, 0.0), Point(4.0, 0.0)))
    val newLink3 = roadLink(3, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val linkmap = Map(1L -> newLink1, 2L -> newLink2, 3L -> newLink3)
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(0.0, 0.0), Point(10.0, 0.0)), 0.0, 10.0, None, None, None, None, 0, None),
      SpeedLimit(
        2, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(0.0, 0.0), Point(10.0, 0.0)), 0.0, 10.0, None, None, None, None, 0, None))

    val changes = Seq(ChangeInfo(Some(1l), Some(1l), 2l, 5, Some(0.0), Some(3.0), Some(0.0), Some(3.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(2l), 22, 6, Some(3.0), Some(7.0), Some(0.0), Some(4.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(3l), 23, 6, Some(7.0), Some(10.0), Some(3.0), Some(0.0), Some(1440000))
    )

    val output = changes map { change =>
      SpeedLimitFiller.projectSpeedLimit(speedLimit.head, linkmap.get(change.newId.get).get,
        Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get)) }
    output.head.sideCode should be (SideCode.TowardsDigitizing)
    output.last.sideCode should be (SideCode.AgainstDigitizing)
    output.head.startMeasure should be(0.0)
    output.head.endMeasure should be(3.0)
    output.last.startMeasure should be(0.0)
    output.last.endMeasure should be(3.0)
    output.foreach(_.value should be (Some(NumericValue(40))))

    val output2 = changes map { change =>
      SpeedLimitFiller.projectSpeedLimit(speedLimit.last, linkmap.get(change.newId.get).get,
        Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get)) }
    output2.length should be(3)
    output2.head.sideCode should be (SideCode.AgainstDigitizing)
    output2.last.sideCode should be (SideCode.TowardsDigitizing)
    output2.head.startMeasure should be(0.0)
    output2.head.endMeasure should be(3.0)
    output2.last.startMeasure should be(0.0)
    output2.last.endMeasure should be(3.0)
    output2.foreach(_.value should be (Some(NumericValue(50))))
  }

  test("project speed limits to new geometry, case 3 - speed changes in the middle of the roadlink") {
    val oldRoadLink = roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0)))
    val newLink1 = roadLink(1, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val newLink2 = roadLink(2, Seq(Point(0.0, 0.0), Point(4.0, 0.0)))
    val newLink3 = roadLink(3, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val linkmap = Map(1L -> newLink1, 2L -> newLink2, 3L -> newLink3)
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(0.0, 0.0), Point(4.5, 0.0)), 0.0, 4.5, None, None, None, None, 0, None),
      SpeedLimit(
        2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(4.5, 0.0), Point(10.0, 0.0)), 4.5, 10.0, None, None, None, None, 0, None))

    val changes = Seq(ChangeInfo(Some(1l), Some(1l), 2l, 5, Some(0.0), Some(3.0), Some(0.0), Some(3.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(2l), 22, 6, Some(3.0), Some(7.0), Some(0.0), Some(4.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(3l), 23, 6, Some(7.0), Some(10.0), Some(3.0), Some(0.0), Some(1440000))
    )

    val output = changes flatMap { change =>
      speedLimit.map(
      SpeedLimitFiller.projectSpeedLimit(_, linkmap.get(change.newId.get).get,
        Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get))) } filter(sl => sl.startMeasure != sl.endMeasure)

    output.foreach(_.sideCode should be (SideCode.BothDirections))
    output.head.startMeasure should be(0.0)
    output.head.endMeasure should be(3.0)
    output.head.value should be(Some(NumericValue(40)))
    output.last.startMeasure should be(0.0)
    output.last.endMeasure should be(3.0)
    output.last.value should be(Some(NumericValue(50)))
    output.length should be (4)
  }

  test("project speed limits to new geometry, case 3b - speed changes in the middle of the roadlink, digitization switches there") {
    val oldRoadLink = roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0)))
    val newLink1 = roadLink(1, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val newLink2 = roadLink(2, Seq(Point(0.0, 0.0), Point(4.0, 0.0)))
    val newLink3 = roadLink(3, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val linkmap = Map(1L -> newLink1, 2L -> newLink2, 3L -> newLink3)
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(0.0, 0.0), Point(7.5, 0.0)), 0.0, 7.5, None, None, None, None, 0, None),
      SpeedLimit(
        2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(7.5, 0.0), Point(10.0, 0.0)), 7.5, 10.0, None, None, None, None, 0, None))

    val changes = Seq(ChangeInfo(Some(1l), Some(1l), 2l, 5, Some(0.0), Some(3.0), Some(0.0), Some(3.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(2l), 22, 6, Some(3.0), Some(7.0), Some(0.0), Some(4.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(3l), 23, 6, Some(7.0), Some(10.0), Some(3.0), Some(0.0), Some(1440000))
    )

    val output = changes flatMap { change =>
      speedLimit.map(
        SpeedLimitFiller.projectSpeedLimit(_, linkmap.get(change.newId.get).get,
          Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get))) } filter(sl => sl.startMeasure != sl.endMeasure)

    output.foreach(_.sideCode should be (SideCode.BothDirections))
    output.length should be (4)
    val (link3sl1, link3sl2) = (output.tail.tail.head, output.last)
    link3sl1.startMeasure should be (2.5)
    link3sl2.startMeasure should be (0)
    link3sl1.endMeasure should be (3)
    link3sl2.endMeasure should be (2.5)
  }

  test("project speed limits to new geometry, case 4 - speed changes in the middle of the roadlink, different for different directions") {
    val oldRoadLink = roadLink(1, Seq(Point(0.0, 0.0), Point(10.0, 0.0)))
    val newLink1 = roadLink(1, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val newLink2 = roadLink(2, Seq(Point(0.0, 0.0), Point(4.0, 0.0)))
    val newLink3 = roadLink(3, Seq(Point(0.0, 0.0), Point(3.0, 0.0)))
    val linkmap = Map(1L -> newLink1, 2L -> newLink2, 3L -> newLink3)
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(0.0, 0.0), Point(4.5, 0.0)), 0.0, 4.5, None, None, None, None, 0, None),
      SpeedLimit(
        2, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(4.5, 0.0), Point(10.0, 0.0)), 4.5, 10.0, None, None, None, None, 0, None),
      SpeedLimit(
        3, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(30)),
        Seq(Point(0.0, 0.0), Point(4.5, 0.0)), 0.0, 4.5, None, None, None, None, 0, None),
      SpeedLimit(
        4, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(4.5, 0.0), Point(10.0, 0.0)), 4.5, 10.0, None, None, None, None, 0, None))

    val changes = Seq(ChangeInfo(Some(1l), Some(1l), 2l, 5, Some(0.0), Some(3.0), Some(0.0), Some(3.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(2l), 22, 6, Some(3.0), Some(7.0), Some(0.0), Some(4.0), Some(1440000)),
      ChangeInfo(Some(1l), Some(3l), 23, 6, Some(7.0), Some(10.0), Some(3.0), Some(0.0), Some(1440000))
    )

    val output = changes flatMap { change =>
      speedLimit.map(
        SpeedLimitFiller.projectSpeedLimit(_, linkmap.get(change.newId.get).get,
          Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get))) } filter(sl => sl.startMeasure != sl.endMeasure)

    output.filter(_.linkId == 1).count(_.value.contains(NumericValue(30))) should be (1)
    output.filter(_.linkId == 1).count(_.value.contains(NumericValue(40))) should be (1)
    output.filter(_.linkId == 2).count(_.value.contains(NumericValue(30))) should be (1)
    output.filter(_.linkId == 2).count(_.value.contains(NumericValue(40))) should be (1)
    output.filter(_.linkId == 2).count(_.value.contains(NumericValue(50))) should be (1)
    output.filter(_.linkId == 2).count(_.value.contains(NumericValue(60))) should be (1)
    output.filter(_.linkId == 3).count(_.value.contains(NumericValue(50))) should be (1)
    output.filter(_.linkId == 3).count(_.value.contains(NumericValue(60))) should be (1)
    output.length should be (8)
  }

  test("test failing combination") {
    val oldRoadLink = roadLink(6599930, Seq(Point(0.0, 0.0), Point(171.02731386, 0.0)))
    val newLink1 = roadLink(6640715, Seq(Point(0.0, 0.0), Point(106.96978931, 0.0)))
    val newLink2 = roadLink(6640718, Seq(Point(0.0, 0.0), Point(57.4888233, 0.0)))
    val newLink3 = roadLink(6640719, Seq(Point(0.0, 0.0), Point(6.56875041, 0.0)))
    val linkmap = Map(6640715L -> newLink1, 6640718L -> newLink2, 6640719L -> newLink3)
    val speedLimit = Seq(
      SpeedLimit(
        1, oldRoadLink.linkId, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(0.0, 0.0), Point(54.825, 0.0)), 0.0, 54.825, None, None, None, None, 0, None),
      SpeedLimit(
        2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(54.825, 0.0), Point(171.027, 0.0)), 54.825, 171.027, None, None, None, None, 0, None))

    val changes = Seq(ChangeInfo(Some(oldRoadLink.linkId), Some(newLink2.linkId), 2l, 5, Some(113.53850795), Some(171.02731386), Some(0.0), Some(57.4888233), Some(1461844024000L)),
      ChangeInfo(Some(oldRoadLink.linkId), Some(newLink1.linkId), 2l, 6, Some(6.56875026), Some(113.53850795), Some(0.0), Some(106.96978931), Some(1461844024000L)),
      ChangeInfo(Some(oldRoadLink.linkId), Some(newLink3.linkId), 2l, 6, Some(0.0), Some(6.56875026), Some(0.0), Some(6.56875026), Some(1461844024000L))
    )

    val output = changes flatMap { change =>
      speedLimit.map(
        SpeedLimitFiller.projectSpeedLimit(_, linkmap.get(change.newId.get).get,
          Projection(change.oldStartMeasure.get, change.oldEndMeasure.get, change.newStartMeasure.get, change.newEndMeasure.get, change.vvhTimeStamp.get))) } filter(sl => sl.startMeasure != sl.endMeasure)

    output.foreach(println)
  }

  test("Should repair speed limit data on overlaps and invalid data") {
    val rLink = roadLink(1, Seq(Point(0.0, 0.0), Point(50.0, 0.0)))
    val bdblue = Option(DateTime.now().minusDays(6))
    val sdblue = Option(DateTime.now().minusDays(5))
    val bdred = Option(DateTime.now().minusDays(4))
    val sdred1 = Option(DateTime.now().minusDays(3).minusHours(1))
    val sdred2 = Option(DateTime.now().minusDays(3))
    val sdred3 = Option(DateTime.now().minusDays(3).plusHours(1))
    val bdgreen = Option(DateTime.now().minusDays(2))
    val sdgreen = Option(DateTime.now().minusDays(1))
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(26.67, 0.0), Point(43.33, 0.0)), 26.67, 43.33, None, None, Some("blue bd"), bdblue, 0, None),
      SpeedLimit(
        2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(10.00, 0.0), Point(26.67, 0.0)), 10.0, 26.67, None, None, Some("red bd"), bdred, 0, None),
      SpeedLimit(
        3, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(50.0, 0.0), Point(60.0, 0.0)), 50.0, 60.0, None, None, Some("green bd"), bdgreen, 0, None),
      SpeedLimit(
        4, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(16.67, 0.0), Point(33.33, 0.0)), 16.67, 33.33, None, None, Some("blue td"), sdblue, 0, None),
      SpeedLimit(
        5, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(23.33, 0.0), Point(36.67, 0.0)), 23.33, 36.67, None, None, Some("blue ad"), sdblue, 0, None),
      SpeedLimit(
        6, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(0.0, 0.0), Point(16.67, 0.0)), 0.0, 16.67, None, None, Some("red td"), sdred1, 0, None),
      SpeedLimit(
        7, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(0.0, 0.0), Point(10.0, 0.0)), 0.0, 10.0, None, None, Some("red ad"), sdred2, 0, None),
      SpeedLimit(
        8, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(10.0, 0.0), Point(23.33, 0.0)), 10.0, 23.33, None, None, Some("red ad"), sdred3, 0, None),
      SpeedLimit(
        9, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(33.33, 0.0), Point(50.0, 0.0)), 33.33, 50.0, None, None, Some("green bd"), sdgreen, 0, None),
      SpeedLimit(
        10, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(40)),
        Seq(Point(36.67, 0.0), Point(50.0, 0.0)), 36.67, 50.0, None, None, Some("green bd"), sdgreen, 0, None)
    )

    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(Seq(rLink), Map(1L -> speedLimit))
//    filledTopology.sortBy(_.startMeasure).foreach(l => println("speed limit:" + l))
//    changeSet.adjustedMValues.foreach(l => println("adjusted MValue:" + l))
//    changeSet.adjustedSideCodes.foreach(l => println("adjusted sidecode:" + l))
//    changeSet.droppedAssetIds.foreach(l => println("dropped id:" + l))
//    changeSet.expiredAssetIds.foreach(l => println("expired id:" + l))

    changeSet.droppedAssetIds should have size 6
    filledTopology.count(_.id != 0) should be (4)
    filledTopology.forall(_.value.nonEmpty) should be (true)
    filledTopology.find(sl => sl.startMeasure == 0.0 && sl.endMeasure == 26.67).get.sideCode should be (SideCode.BothDirections)
    filledTopology.find(sl => sl.startMeasure == 0.0 && sl.endMeasure == 26.67).get.value.get should be (NumericValue(50))

    filledTopology.find(sl => sl.startMeasure == 36.67 && sl.endMeasure == 50.0).get.sideCode should be (SideCode.BothDirections)
    filledTopology.find(sl => sl.startMeasure == 36.67 && sl.endMeasure == 50.0).get.value.get should be (NumericValue(40))

    // Test that filler is stable
    val (refill, newChangeSet) = SpeedLimitFiller.fillTopology(Seq(rLink), Map(1L -> filledTopology.map(sl => sl.copy(id = sl.id+1))))
    refill should have size filledTopology.size
    // Except for ids these must be equal
    refill.forall(sl => filledTopology.find(_.id == sl.id-1).get.copy(id = sl.id).equals(sl))
    newChangeSet.adjustedMValues should have size 0
    newChangeSet.droppedAssetIds should have size 0
    newChangeSet.adjustedSideCodes should have size 0
    newChangeSet.expiredAssetIds should have size 0
  }

  test("Should split older asset if necessary") {
    val rLink = roadLink(1, Seq(Point(0.0, 0.0), Point(50.0, 0.0)))
    val linkmap = Map(1L -> rLink)
    val edit1 = Option(DateTime.now().minusDays(7))
    val edit2 = Option(DateTime.now().minusDays(6))
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(0.0, 0.0), Point(50.0, 0.0)), 0.0, 50.0, None, None, Some("blue bd"), edit1, 0, None),
      SpeedLimit(
        2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(10.00, 0.0), Point(26.67, 0.0)), 10.0, 26.67, None, None, Some("red bd"), edit2, 0, None)    )

    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(Seq(rLink), Map(1L -> speedLimit))
//    filledTopology.sortBy(_.startMeasure).foreach(l => println("speed limit:" + l))
//    changeSet.adjustedMValues.foreach(l => println("adjusted MValue:" + l))
//    changeSet.adjustedSideCodes.foreach(l => println("adjusted sidecode:" + l))
//    changeSet.droppedAssetIds.foreach(l => println("dropped id:" + l))
//    changeSet.expiredAssetIds.foreach(l => println("expired id:" + l))

    changeSet.droppedAssetIds should be (Set())
    filledTopology.length should be (3)
    filledTopology.map(_.id).toSet should be (Set(0,1,2))
    val newLink = filledTopology.find(_.startMeasure==0.0).get
    newLink.endMeasure should be (10.0)
    newLink.value.get should be (NumericValue(60))
    val oldLink1 = filledTopology.find(_.startMeasure==26.67).get
    oldLink1.endMeasure should be (50.0)
    oldLink1.value should be (Some(NumericValue(60)))
    val oldLink2 = filledTopology.find(_.startMeasure==10.0).get
    oldLink2.startMeasure should be (10.0)
    oldLink2.endMeasure should be (26.67)
    oldLink2.value should be (Some(NumericValue(50)))

    val (refill, newChangeSet) = SpeedLimitFiller.fillTopology(Seq(rLink), Map(1L -> filledTopology))
    refill should have size 3
    newChangeSet.adjustedMValues should have size 0
  }

  test("Should fill any holes it creates") {
    val rLink = roadLink(1, Seq(Point(0.0, 0.0), Point(50.0, 0.0)))
    val linkmap = Map(1L -> rLink)
    val edit1 = Option(DateTime.now().minusDays(7))
    val edit2 = Option(DateTime.now().minusDays(6))
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(0.0, 0.0), Point(50.0, 0.0)), 0.0, 50.0, None, None, Some("blue bd"), edit1, 0, None),
      SpeedLimit(
        2, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(10.00, 0.0), Point(26.67, 0.0)), 10.0, 26.7, None, None, Some("red bd"), edit2, 0, None),
      SpeedLimit(
        3, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(26.8, 0.0), Point(50, 0.0)), 26.74, 50, None, None, Some("red bd"), edit2, 0, None)
    )

    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(Seq(rLink), Map(1L -> speedLimit))
//    filledTopology.sortBy(_.startMeasure).foreach(l => println("speed limit:" + l))
//    changeSet.adjustedMValues.foreach(l => println("adjusted MValue:" + l))
//    changeSet.adjustedSideCodes.foreach(l => println("adjusted sidecode:" + l))
//    changeSet.droppedAssetIds.foreach(l => println("dropped id:" + l))
//    changeSet.expiredAssetIds.foreach(l => println("expired id:" + l))

    changeSet.droppedAssetIds should be (Set())
    filledTopology.length should be (3)
    val oldLink0 = filledTopology.find(_.startMeasure==0.0).get
    oldLink0.endMeasure should be (10.0)
    oldLink0.value.get should be (NumericValue(60))
    val oldLink1 = filledTopology.find(_.startMeasure==10.0).get
    oldLink1.endMeasure should be (26.74)
    oldLink1.value.get should be (NumericValue(50))
    val oldLink2 = filledTopology.find(_.startMeasure==26.74).get
    oldLink2.endMeasure should be (50.0)
    oldLink2.value.get should be (NumericValue(50))
  }


  test("should not combine different values on opposite directions") {
    val rLink = roadLink(1, Seq(Point(0.0, 0.0), Point(50.0, 0.0)))
    val edit1 = Option(DateTime.now().minusDays(7))
    val edit2 = Option(DateTime.now().minusDays(6))
    val speedLimit = Seq(
      SpeedLimit(
        1, 1, SideCode.BothDirections, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(0.0, 0.0), Point(30.0, 0.0)), 0.0, 30.0, None, None, Some("blue bd"), edit1, 0, None),
      SpeedLimit(
        0, 1, SideCode.TowardsDigitizing, TrafficDirection.BothDirections, Some(NumericValue(50)),
        Seq(Point(30.00, 0.0), Point(50.0, 0.0)), 30.0, 50.0, None, None, Some("red td"), edit2, 0, None),
      SpeedLimit(
        0, 1, SideCode.AgainstDigitizing, TrafficDirection.BothDirections, Some(NumericValue(60)),
        Seq(Point(30.0, 0.0), Point(50.0, 0.0)), 30.0, 50.0, None, None, Some("blue ad"), edit2, 0, None)
    )

    val (filledTopology, changeSet) = SpeedLimitFiller.fillTopology(Seq(rLink), Map(1L -> speedLimit))
//    filledTopology.sortBy(_.startMeasure).foreach(l => println("speed limit:" + l))
//    changeSet.adjustedMValues.foreach(l => println("adjusted MValue:" + l))
//    changeSet.adjustedSideCodes.foreach(l => println("adjusted sidecode:" + l))
//    changeSet.droppedAssetIds.foreach(l => println("dropped id:" + l))
//    changeSet.expiredAssetIds.foreach(l => println("expired id:" + l))

    filledTopology.length should be (3)
    val oldLink1 = filledTopology.find(_.value.contains(NumericValue(50))).get
    oldLink1.startMeasure should be (30.0)
    oldLink1.endMeasure should be (50.00)
    val oldLink2 = filledTopology.find(_.createdBy.contains("blue ad")).get
    oldLink2.startMeasure should be (30.0)
    oldLink2.endMeasure should be (50.00)
    oldLink2.value.get should be (NumericValue(60))
  }

  case class ChangeInfo(oldId: Option[Long],
                        newId: Option[Long],
                        mmlId: Long,
                        changeType: Int,
                        oldStartMeasure: Option[Double],
                        oldEndMeasure: Option[Double],
                        newStartMeasure: Option[Double],
                        newEndMeasure: Option[Double],
                        vvhTimeStamp: Option[Long])
}
