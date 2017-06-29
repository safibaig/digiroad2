package fi.liikennevirasto.viite.process

import fi.liikennevirasto.digiroad2.{ChangeInfo, Point}
import fi.liikennevirasto.digiroad2.asset.SideCode
import fi.liikennevirasto.digiroad2.util.Track
import fi.liikennevirasto.viite.NewRoadAddress
import fi.liikennevirasto.viite.dao.{Discontinuity, RoadAddress}
import fi.liikennevirasto.viite.util.prettyPrint
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}


class RoadAddressChangeInfoMapperSpec extends FunSuite with Matchers {
  test("resolve simple case") {
    val roadAddress = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 0, 1000, Some(DateTime.now), None,
      None, 0L, 123L, 0.0, 1000.234, SideCode.AgainstDigitizing, 86400L, (None, None), false, Seq(Point(0.0, 0.0), Point(1000.234, 0.0)))
    val roadAddress2 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 1000, 1400, Some(DateTime.now), None,
      None, 0L, 124L, 0.0, 399.648, SideCode.AgainstDigitizing, 86400L, (None, None), false, Seq(Point(1000.234, 0.0), Point(1000.234, 399.648)))
    val map = Seq(roadAddress, roadAddress2).groupBy(_.linkId)
    val changes = Seq(
      ChangeInfo(Some(123), Some(124), 123L, 2, Some(0.0), Some(1000.234), Some(399.648), Some(1399.882), 96400L),
      ChangeInfo(Some(124), Some(124), 123L, 1, Some(0.0), Some(399.648), Some(0.0), Some(399.648), 96400L))
    val results = RoadAddressChangeInfoMapper.resolveChangesToMap(map, Seq(), changes)
    results.get(123).isEmpty should be (true)
    results.get(124).isEmpty should be (false)
    results(124).size should be (2)
    results.values.flatten.exists(_.startAddrMValue == 0) should be (true)
    results.values.flatten.exists(_.startAddrMValue == 1000) should be (true)
    results.values.flatten.exists(_.endAddrMValue == 1000) should be (true)
    results.values.flatten.exists(_.endAddrMValue == 1400) should be (true)
    results.values.flatten.forall(_.adjustedTimestamp == 96400L) should be (true)
  }

  test("transfer 1 to 2, modify 2, transfer 2 to 3") {
    val roadLinkId1 = 123L
    val roadLinkId2 = 456L
    val roadLinkId3 = 789L

    val roadAdjustedTimestamp = 0L
    val changesVVHTimestamp = 96400L

    val roadAddress1 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 0, 1000, Some(DateTime.now), None,
      None, 0L, roadLinkId1, 0.0, 1000.234, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(0.0, 0.0), Point(1000.234, 0.0)))
    val roadAddress2 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 1000, 1400, Some(DateTime.now), None,
      None, 0L, roadLinkId2, 0.0, 399.648, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(1000.234, 0.0), Point(1000.234, 399.648)))
    val roadAddress3 = RoadAddress(367,75,2,Track.Combined,Discontinuity.Continuous,3532,3598,None,None,
      Some("tr"),70000389,roadLinkId3,0.0,65.259,SideCode.TowardsDigitizing,roadAdjustedTimestamp,(None,None),true,List(Point(538889.668,6999800.979,0.0), Point(538912.266,6999862.199,0.0)))
    val map = Seq(roadAddress1, roadAddress2, roadAddress3).groupBy(_.linkId)
    val changes = Seq(
      ChangeInfo(Some(roadLinkId1), Some(roadLinkId2), 123L, 2, Some(0.0), Some(1000.234), Some(399.648), Some(1399.882), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId2), Some(roadLinkId2), 123L, 1, Some(0.0), Some(399.648), Some(0.0), Some(399.648), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId2), Some(roadLinkId3), 123L, 2, Some(0.0), Some(6666), Some(200), Some(590), changesVVHTimestamp)
    )
    val results = RoadAddressChangeInfoMapper.resolveChangesToMap(map, Seq(), changes)
    results.get(roadLinkId1).isEmpty should be (true)
    results.get(roadLinkId2).isEmpty should be (false)
    results.get(roadLinkId3).isEmpty should be (false)
    results(roadLinkId2).size should be (2)
    results(roadLinkId2).count(_.id == -1000) should be (2)
    results(roadLinkId2).count(rl => rl.id == -1000 && (rl.startAddrMValue == 0 || rl.startAddrMValue == 1000) && (rl.endAddrMValue == 1000 || rl.endAddrMValue == 1400)) should be (2)
    results(roadLinkId3).size should be (2)
    results(roadLinkId3).count(_.id == -1000) should be (1)
    results(roadLinkId3).count(_.id != -1000) should be (1)
    results(roadLinkId3).filter(_.id != -1000).head.eq(roadAddress3) should be (true)
    results.values.flatten.exists(_.startAddrMValue == 0) should be (true)
    results.values.flatten.exists(_.startAddrMValue == 1000) should be (true)
    results.values.flatten.exists(_.endAddrMValue == 1000) should be (true)
    results.values.flatten.exists(_.endAddrMValue == 1400) should be (true)
  }

  test("no changes should apply") {
    val roadLinkId1 = 123L
    val roadLinkId2 = 456L
    val roadLinkId3 = 789L

    val roadAdjustedTimestamp = 964000L
    val changesVVHTimestamp = 96400L

    val roadAddress1 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 0, 1000, Some(DateTime.now), None,
      None, 0L, roadLinkId1, 0.0, 1000.234, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(0.0, 0.0), Point(1000.234, 0.0)))
    val roadAddress2 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 1000, 1400, Some(DateTime.now), None,
      None, 0L, roadLinkId2, 0.0, 399.648, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(1000.234, 0.0), Point(1000.234, 399.648)))
    val roadAddress3 = RoadAddress(367,75,2,Track.Combined,Discontinuity.Continuous,3532,3598,None,None,
      Some("tr"),70000389,roadLinkId3,0.0,65.259,SideCode.TowardsDigitizing,roadAdjustedTimestamp,(None,None),true,List(Point(538889.668,6999800.979,0.0), Point(538912.266,6999862.199,0.0)))
    val map = Seq(roadAddress1, roadAddress2, roadAddress3).groupBy(_.linkId)
    val changes = Seq(
      ChangeInfo(Some(roadLinkId1), Some(roadLinkId2), 123L, 2, Some(0.0), Some(1000.234), Some(399.648), Some(1399.882), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId2), Some(roadLinkId2), 123L, 1, Some(0.0), Some(399.648), Some(0.0), Some(399.648), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId2), Some(roadLinkId3), 123L, 2, Some(0.0), Some(6666), Some(200), Some(590), changesVVHTimestamp)
    )
    val results = RoadAddressChangeInfoMapper.resolveChangesToMap(map, Seq(), changes)
    results.get(roadLinkId1).isEmpty should be (false)
    results.get(roadLinkId2).isEmpty should be (false)
    results.get(roadLinkId3).isEmpty should be (false)
    results(roadLinkId1).size should be (1)
    results(roadLinkId1).count(_.id == -1000) should be (0)
    results(roadLinkId1).head.eq(roadAddress1) should be (true)
    results(roadLinkId2).size should be (1)
    results(roadLinkId2).count(_.id == -1000) should be (0)
    results(roadLinkId2).head.eq(roadAddress2) should be (true)
    results(roadLinkId3).size should be (1)
    results(roadLinkId3).count(_.id == -1000) should be (0)
    results(roadLinkId3).head.eq(roadAddress3) should be (true)
  }

  test("modify 1, transfer 2 To 1, transfer 3 to 1") {
    val roadLinkId1 = 123L
    val roadLinkId2 = 456L
    val roadLinkId3 = 789L

    val roadAdjustedTimestamp = 0L
    val changesVVHTimestamp = 96400L

    val roadAddress1 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 400, 1400, Some(DateTime.now), None,
      None, 0L, roadLinkId1, 0.0, 1000.234, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(0.0, 0.0), Point(1000.234, 0.0)))
    val roadAddress2 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, 0, 400, Some(DateTime.now), None,
      None, 0L, roadLinkId2, 0.0, 399.648, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(1000.234, 0.0), Point(1000.234, 399.648)))
    val roadAddress3 = RoadAddress(367,75,2,Track.Combined,Discontinuity.Continuous,1400,1465,None,None,
      Some("tr"),70000389,roadLinkId3,0.0,65.259,SideCode.TowardsDigitizing,roadAdjustedTimestamp,(None,None),true,List(Point(538889.668,6999800.979,0.0), Point(538912.266,6999862.199,0.0)))
    val map = Seq(roadAddress1, roadAddress2, roadAddress3).groupBy(_.linkId)
    val changes = Seq(
      //Modifications
      ChangeInfo(Some(roadLinkId1), Some(roadLinkId1), 123L, 1, Some(0.0), Some(1000.234), Some(399.648), Some(1399.882), changesVVHTimestamp),
      //Transferings
      ChangeInfo(Some(roadLinkId2), Some(roadLinkId1), 123L, 2, Some(0.0), Some(399.648), Some(0.0), Some(399.648), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId3), Some(roadLinkId1), 123L, 2, Some(0.0), Some(65.259), Some(1465.0), Some(1399.882), changesVVHTimestamp)
    )
    val results = RoadAddressChangeInfoMapper.resolveChangesToMap(map, Seq(), changes).mapValues(_.sortBy(_.startAddrMValue))
    results.get(roadLinkId1).isEmpty should be (false)
    results.get(roadLinkId2).isEmpty should be (true)
    results.get(roadLinkId3).isEmpty should be (true)
    results(roadLinkId1).size should be (3)
    results(roadLinkId1).forall(_.id == NewRoadAddress) should be (true)
    val addr1 = results(roadLinkId1)(0)
    addr1.startMValue should be (0)
    addr1.endMValue should be (399.648)
    addr1.startAddrMValue should be (0)
    addr1.endAddrMValue should be (400)
    addr1.adjustedTimestamp should be (changesVVHTimestamp)
    val addr2 = results(roadLinkId1)(1)
    addr2.startMValue should be (399.648)
    addr2.endMValue should be (1399.882)
    addr2.startAddrMValue should be (400)
    addr2.endAddrMValue should be (1400)
    addr2.adjustedTimestamp should be (changesVVHTimestamp)
    val addr3 = results(roadLinkId1)(2)
    addr3.startMValue should be (1399.882)
    addr3.endMValue should be (1465.0)
    addr3.startAddrMValue should be (1400)
    addr3.endAddrMValue should be (1465)
    addr3.adjustedTimestamp should be (changesVVHTimestamp)
  }

  test("Simple division case"){
    val roadLinkId1 = 12345L
    val roadLinkId2 = 12346L
    val roadLinkId3 = 12347L

    //Mvalues
    val startM1 = 0.0
    val endM1 = 48.785
    val startAddrM1 = 4668L
    val endAddrM1 = 4717L

    val startM2 = 48.785
    val endM2 = 61.231
    val startAddrM2 = 4717
    val endAddrM2 = 4729

    //01-01-1976 converted to UNIX TIMESTAMP 189302400
    val roadAdjustedTimestamp = 189302400L
    val changesVVHTimestamp = 1496818210L

    val roadAddress1 = RoadAddress(1, 1, 1, Track.RightSide, Discontinuity.Continuous, startAddrM1, endAddrM1, Some(DateTime.now), None,
      None, 0L, roadLinkId1, startM1, endM1, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(0.000, 0.000), Point(28.000, -7.896), Point(48.200, -10.629), Point(48.200, 1.126)))
    val roadAddress2 = RoadAddress(2, 1, 1, Track.RightSide, Discontinuity.Continuous, startAddrM2, endAddrM2, Some(DateTime.now), None,
      None, 0L, roadLinkId1, startM2, endM2, SideCode.AgainstDigitizing, roadAdjustedTimestamp, (None, None), false, Seq(Point(0.000, 0.000), Point(28.000, -7.896), Point(48.200, -10.629), Point(48.200, 1.126)))
    val map = Seq(roadAddress1, roadAddress2).groupBy(_.linkId)
    val changes = Seq(
      //Modifications
      ChangeInfo(Some(roadLinkId1), Some(roadLinkId1), 6543L, 4, Some(20.384), Some(49.476), Some(29.092), Some(0.0), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId1), Some(roadLinkId2), 6544L, 5, Some(0.0), Some(20.384), Some(20.384), Some(0.0), changesVVHTimestamp),
      ChangeInfo(Some(roadLinkId1), Some(roadLinkId3), 6545L, 5, Some(49.476), Some(61.231), Some(0.0), Some(11.755), changesVVHTimestamp)
    )

    val results = RoadAddressChangeInfoMapper.resolveChangesToMap(map, Seq(), changes).mapValues(_.sortBy(_.startAddrMValue))
    results.size should be (1)
    results.get(roadLinkId1).isEmpty should be (false)
    results.get(roadLinkId2).isEmpty should be (false)
    results.get(roadLinkId3).isEmpty should be (false)
    val addr1 = results(roadLinkId1)
    val addr2 = results(roadLinkId2)
    val addr3 = results(roadLinkId3)

    addr1.size should be (2)
    results.flatMap(_._2).filter(_.floating).size should be (3)
    results.flatMap(_._2).filter(_.floating).map(_.linkId) should contain allOf (roadLinkId1, roadLinkId2, roadLinkId3)

    addr1.filterNot(_.endDate.isEmpty).head.geom should contain (Point(28.000, -7.896))
    addr2.size should be (1)
    addr2.head.geom should contain (Point(0.000, 0.000))
    addr3.size should be (1)
    addr3.head.geom should contain (Point(48.200, 1.126))

  }


}
