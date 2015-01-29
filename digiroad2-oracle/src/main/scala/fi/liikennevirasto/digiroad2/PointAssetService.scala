package fi.liikennevirasto.digiroad2

import fi.liikennevirasto.digiroad2.ConversionDatabase._

import scala.slick.driver.JdbcDriver.backend.Database
import scala.slick.jdbc.StaticQuery.interpolation
import scala.slick.driver.JdbcDriver.backend.Database.dynamicSession

object PointAssetService {
  def getByMunicipality(typeId: Int, municipalityNumber: Int): Seq[Map[String, Any]] = {
    Database.forDataSource(dataSource).withDynTransaction {
      val query = sql"""
         select s.segm_id, s.tielinkki_id, to_2d(sdo_lrs.dynamic_segment(t.shape, s.alkum, s.loppum)), s.puoli
           from segments s
           join tielinkki_ctas t on s.tielinkki_id = t.dr1_id
           where t.kunta_nro = $municipalityNumber and s.tyyppi = $typeId
        """
      query.as[(Long, Long, Seq[Point], Int)].iterator().map {
        case (id, roadLinkId, geometry, sideCode) => Map("id" -> id, "point" -> geometry.head, "sideCode" -> sideCode)
      }.toSeq
    }
  }

  def getDirectionalTrafficSignsByMunicipality(municipalityNumber: Int): Seq[Map[String, Any]] = {
    Database.forDataSource(dataSource).withDynTransaction {
      val query = sql"""
         select s.segm_id, s.tielinkki_id, to_2d(sdo_lrs.dynamic_segment(t.shape, s.alkum, s.loppum)), s.puoli, s.opas_teksti
           from segm_opastaulu s
           join tielinkki_ctas t on s.tielinkki_id = t.dr1_id
           where t.kunta_nro = $municipalityNumber
        """
      query.as[(Long, Long, Seq[Point], Int, String)].iterator().map {
        case (id, roadLinkId, geometry, sideCode, infoText) => Map("id" -> id, "point" -> geometry.head, "sideCode" -> sideCode, "infoText" -> infoText)
      }.toSeq
    }
  }

  def getRailwayCrossingsByMunicipality(municipalityNumber: Int): Seq[Map[String, Any]] = {
    Database.forDataSource(dataSource).withDynTransaction {
      val query = sql"""
         select s.segm_id, s.tielinkki_id, to_2d(sdo_lrs.dynamic_segment(t.shape, s.alkum, s.loppum)), s.puoli, s.varustus, s.nimi_s, s.nimi_r
           from segm_tasoristeys s
           join tielinkki_ctas t on s.tielinkki_id = t.dr1_id
           where t.kunta_nro = $municipalityNumber
        """
      query.as[(Long, Long, Seq[Point], Int, Int, String, String)].iterator().map {
        case (id, roadLinkId, geometry, sideCode, props, nameFi, nameSv) =>
          Map("id" -> id, "point" -> geometry.head,
              "sideCode" -> sideCode, "props" -> props,
              "nameFi" -> nameFi, "nameSv" -> nameSv)
      }.toSeq
    }
  }
}
