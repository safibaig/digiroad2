package fi.liikennevirasto.digiroad2

import fi.liikennevirasto.digiroad2.asset.Modification
import fi.liikennevirasto.digiroad2.linearasset.{ValidityPeriodDayOfWeek, ProhibitionValidityPeriod}
import org.json4s.{Formats, DefaultFormats}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Tag, FunSuite}
import org.scalatra.test.scalatest.ScalatraSuite
import org.apache.commons.codec.binary.Base64
import org.json4s.jackson.JsonMethods._


class IntegrationApiSpec extends FunSuite with ScalatraSuite {
  protected implicit val jsonFormats: Formats = DefaultFormats
  def stopWithMmlId(mmlId: Long): MassTransitStopWithTimeStamps = {
    MassTransitStopWithTimeStamps(1L, 2L, 1.0, 2.0, None, false, Modification(None, None), Modification(None, None), Some(mmlId), None, Seq())
  }
  val mockMasstTransitStopService = MockitoSugar.mock[MassTransitStopService]
  when(mockMasstTransitStopService.getByMunicipality(235)).thenReturn(Seq(stopWithMmlId(123L), stopWithMmlId(321L)))
  private val integrationApi = new IntegrationApi(mockMasstTransitStopService)
  addServlet(integrationApi, "/*")

  def getWithBasicUserAuth[A](uri: String, username: String, password: String)(f: => A): A = {
    val credentials = username + ":" + password
    val encodedCredentials = Base64.encodeBase64URLSafeString(credentials.getBytes)
    val authorizationToken = "Basic " + encodedCredentials + "="
    get(uri, Seq.empty, Map("Authorization" -> authorizationToken))(f)
  }

  test("Should require correct authentication", Tag("db")) {
    get("/mass_transit_stops") {
      status should equal(401)
    }
    getWithBasicUserAuth("/mass_transit_stops", "nonexisting", "incorrect") {
      status should equal(401)
    }
  }

  test("Get assets requires municipality number") {
    getWithBasicUserAuth("/mass_transit_stops", "kalpa", "kalpa") {
      status should equal(400)
    }
    getWithBasicUserAuth("/mass_transit_stops?municipality=235", "kalpa", "kalpa") {
      status should equal(200)
    }
  }

  test("Returns mml id of the road link that the stop refers to") {
    getWithBasicUserAuth("/mass_transit_stops?municipality=235", "kalpa", "kalpa") {
      val mmlIds = (((parse(body) \ "features") \ "properties") \ "mml_id").extract[Seq[Long]]
      mmlIds should be(Seq(123L, 321L))
    }
  }

  test("encode validity period to time domain") {
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(6, 10, ValidityPeriodDayOfWeek.Weekday)) should be("[(t2h6){d5h4}]")
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(23, 24, ValidityPeriodDayOfWeek.Weekday)) should be("[(t2h23){d5h1}]")
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(21, 7, ValidityPeriodDayOfWeek.Saturday)) should be("[(t7h21){h10}]")
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(21, 7, ValidityPeriodDayOfWeek.Sunday)) should be("[(t1h21){h10}]")
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(0, 24, ValidityPeriodDayOfWeek.Weekday)) should be("[(t2h0){d5h24}]")
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(0, 24, ValidityPeriodDayOfWeek.Saturday)) should be("[(t7h0){h24}]")
    integrationApi.toTimeDomain(ProhibitionValidityPeriod(0, 24, ValidityPeriodDayOfWeek.Sunday)) should be("[(t1h0){h24}]")
  }
}
