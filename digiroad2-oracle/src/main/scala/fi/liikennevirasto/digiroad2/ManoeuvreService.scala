package fi.liikennevirasto.digiroad2

import fi.liikennevirasto.digiroad2.asset.BoundingRectangle
import fi.liikennevirasto.digiroad2.asset.oracle.Queries._
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.oracle.collections.OracleArray
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.slick.driver.JdbcDriver.backend.Database
import scala.slick.driver.JdbcDriver.backend.Database.dynamicSession
import scala.slick.jdbc.{StaticQuery => Q}

case class Manoeuvre(id: Long, sourceRoadLinkId: Long, destRoadLinkId: Long, sourceMmlId: Long, destMmlId: Long)

object ManoeuvreService {
  def getByMunicipality(municipalityNumber: Int): Seq[Manoeuvre] = {
    Database.forDataSource(OracleDatabase.ds).withDynTransaction {
      val roadLinks = RoadLinkService.getByMunicipalityWithProperties(municipalityNumber)
        .map(link => (link("id").asInstanceOf[Long], link("mmlId").asInstanceOf[Long]))
        .toMap

      getByRoadlinks(roadLinks)
    }
  }

  def getByBoundingBox(bounds: BoundingRectangle, municipalities: Set[Int]): Seq[Manoeuvre] = {
    Database.forDataSource(OracleDatabase.ds).withDynTransaction {
      val roadLinks = RoadLinkService.getRoadLinks(bounds, municipalities)
        .map(link => (link._1, link._2))
        .toMap

      getByRoadlinks(roadLinks)
    }
  }

  private def getByRoadlinks(roadLinks: Map[Long,Long]): Seq[Manoeuvre] = {
    val FirstElement = 1
    val LastElement = 3

    val manoeuvres = OracleArray.fetchManoeuvresByRoadLinkIds(roadLinks.keys.toList, bonecpToInternalConnection(dynamicSession.conn))

    val manoeuvresById: Map[Long, Seq[(Long, Int, Long, Int, DateTime, String)]] = manoeuvres.toList.groupBy(_._1)
    manoeuvresById.filter { case (id, links) =>
      links.size == 2 && links.exists(_._4 == 1) && links.exists(_._4 == 3)
    }.map { case (id, links) =>
      val (_, _, sourceRoadLinkId, _, _, _) = links.find(_._4 == FirstElement).get
      val (_, _, destRoadLinkId, _, _, _) = links.find(_._4 == LastElement).get
      val sourceMmlId = roadLinks.getOrElse(sourceRoadLinkId, RoadLinkService.getRoadLink(sourceRoadLinkId)._2)
      val destMmlId = roadLinks.getOrElse(destRoadLinkId, RoadLinkService.getRoadLink(destRoadLinkId)._2)

      Manoeuvre(id, sourceRoadLinkId, destRoadLinkId, sourceMmlId, destMmlId)
    }.toSeq
  }

}
