package fi.liikennevirasto.digiroad2.asset.oracle

import com.github.tototoshi.slick.MySQLJodaSupport._
import org.joda.time.DateTime
import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{StaticQuery => Q}


class OracleAssetDao {
  def getLastExecutionDate(typeId: Int, createdBy: String): DateTime = {
    sql"""
      select MAX(a.created_date)
      from asset a
      where a.created_by = $createdBy
        and a.asset_type_id = $typeId
    """.as[DateTime].first
  }
}
