package fi.liikennevirasto.digiroad2.masstransitstop.oracle

import java.sql.SQLException

import _root_.oracle.spatial.geometry.JGeometry

import slick.driver.JdbcDriver.backend.Database
import Database.dynamicSession
import fi.liikennevirasto.digiroad2.{FloatingReason, MassTransitStopRow, Point, RoadLinkService}
import fi.liikennevirasto.digiroad2.asset.PropertyTypes._
import fi.liikennevirasto.digiroad2.asset.{MassTransitStopValidityPeriod, _}
import fi.liikennevirasto.digiroad2.masstransitstop.MassTransitStopOperations
import fi.liikennevirasto.digiroad2.masstransitstop.oracle.Queries._
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase._
import fi.liikennevirasto.digiroad2.user.User
import org.joda.time.{DateTime, Interval, LocalDate}
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory
import scala.language.reflectiveCalls
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{GetResult, PositionedParameters, PositionedResult, SetParameter, StaticQuery => Q}

class MassTransitStopDao {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val SetStringSeq: SetParameter[IndexedSeq[Any]] = new SetParameter[IndexedSeq[Any]] {
    def apply(seq: IndexedSeq[Any], p: PositionedParameters): Unit = {
      for (i <- 1 to seq.length) {
        p.ps.setObject(i, seq(i - 1))
      }
    }
  }

  def getNationalBusStopId = {
    nextNationalBusStopId.as[Long].first
  }

  def assetRowToProperty(assetRows: Iterable[MassTransitStopRow]): Seq[Property] = {
    assetRows.groupBy(_.property.propertyId).map { case (key, rows) =>
      val row = rows.head
      Property(
        id = key,
        publicId = row.property.publicId,
        propertyType = row.property.propertyType,
        required = row.property.propertyRequired,
        values = rows.map(assetRow =>
          PropertyValue(
            assetRow.property.propertyValue,
            propertyDisplayValueFromAssetRow(assetRow))
        ).filter(_.propertyDisplayValue.isDefined).toSeq)
    }.toSeq
  }

  private def propertyDisplayValueFromAssetRow(assetRow: MassTransitStopRow): Option[String] = {
    if (assetRow.property.publicId == "liikennointisuuntima") Some(getBearingDescription(assetRow.validityDirection, assetRow.bearing))
    else Option(assetRow.property.propertyDisplayValue)
  }

  private[oracle] def getBearingDescription(validityDirection: Int, bearing: Option[Int]): String = {
    MassTransitStopOperations.calculateActualBearing(validityDirection, bearing).getOrElse(0) match {
      case x if 46 to 135 contains x => "Itä"
      case x if 136 to 225 contains x => "Etelä"
      case x if 226 to 315 contains x => "Länsi"
      case _ => "Pohjoinen"
    }
  }

  def updateAssetLastModified(assetId: Long, modifier: String) {
    updateAssetModified(assetId, modifier).execute
  }

  private def validPropertyUpdates(propertyWithType: Tuple3[String, Option[Long], SimpleProperty]): Boolean = {
    propertyWithType match {
      case (SingleChoice, _, property) => property.values.nonEmpty
      case _ => true
    }
  }

  private def propertyWithTypeAndId(property: SimpleProperty): Tuple3[String, Option[Long], SimpleProperty] = {
    if (AssetPropertyConfiguration.commonAssetProperties.get(property.publicId).isDefined) {
      (AssetPropertyConfiguration.commonAssetProperties(property.publicId).propertyType, None, property)
    }
    else {
      val propertyId = Q.query[String, Long](propertyIdByPublicId).apply(property.publicId).firstOption.getOrElse(throw new IllegalArgumentException("Property: " + property.publicId + " not found"))
      (Q.query[Long, String](propertyTypeByPropertyId).apply(propertyId).first, Some(propertyId), property)
    }
  }

  def updateAssetProperties(assetId: Long, properties: Seq[SimpleProperty]) {
    properties.map(propertyWithTypeAndId).filter(validPropertyUpdates).foreach { propertyWithTypeAndId =>
      if (AssetPropertyConfiguration.commonAssetProperties.get(propertyWithTypeAndId._3.publicId).isDefined) {
        updateCommonAssetProperty(assetId, propertyWithTypeAndId._3.publicId, propertyWithTypeAndId._1, propertyWithTypeAndId._3.values)
      } else {
        updateAssetSpecificProperty(assetId, propertyWithTypeAndId._3.publicId, propertyWithTypeAndId._2.get, propertyWithTypeAndId._1, propertyWithTypeAndId._3.values)
      }
    }
  }

  private def updateAssetSpecificProperty(assetId: Long, propertyPublicId: String, propertyId: Long, propertyType: String, propertyValues: Seq[PropertyValue]) {
    propertyType match {
      case Text | LongText => {
        if (propertyValues.size > 1) throw new IllegalArgumentException("Text property must have exactly one value: " + propertyValues)
        if (propertyValues.isEmpty) {
          deleteTextProperty(assetId, propertyId).execute
        } else if (textPropertyValueDoesNotExist(assetId, propertyId)) {
          insertTextProperty(assetId, propertyId, propertyValues.head.propertyValue).execute
        } else {
          updateTextProperty(assetId, propertyId, propertyValues.head.propertyValue).execute
        }
      }
      case SingleChoice => {
        if (propertyValues.size != 1) throw new IllegalArgumentException("Single choice property must have exactly one value. publicId: " + propertyPublicId)
        if (singleChoiceValueDoesNotExist(assetId, propertyId)) {
          insertSingleChoiceProperty(assetId, propertyId, propertyValues.head.propertyValue.toLong).execute
        } else {
          updateSingleChoiceProperty(assetId, propertyId, propertyValues.head.propertyValue.toLong).execute
        }
      }
      case MultipleChoice => {
        createOrUpdateMultipleChoiceProperty(propertyValues, assetId, propertyId)
      }
      case ReadOnly | ReadOnlyNumber | ReadOnlyText => {
        logger.debug("Ignoring read only property in update: " + propertyPublicId)
      }
      case t: String => throw new UnsupportedOperationException("Asset property type: " + t + " not supported")
    }
  }

  def updateTextPropertyValue(assetId: Long, propertyPublicId: String, value: String): Unit = {
    val propertyId = Q.query[String, Long](propertyIdByPublicId).apply(propertyPublicId).firstOption.getOrElse(throw new IllegalArgumentException("Property: " + propertyPublicId + " not found"))
    if (textPropertyValueDoesNotExist(assetId, propertyId)) {
      insertTextProperty(assetId, propertyId, value).execute
    } else {
      updateTextProperty(assetId, propertyId, value).execute
    }
  }

  def updateNumberPropertyValue(assetId: Long, propertyPublicId: String, value: Int): Unit = {
    val propertyId = Q.query[String, Long](propertyIdByPublicId).apply(propertyPublicId).firstOption.getOrElse(throw new IllegalArgumentException("Property: " + propertyPublicId + " not found"))
    if (numberPropertyValueDoesNotExist(assetId, propertyId)) {
      insertNumberProperty(assetId, propertyId, value).execute
    } else {
      updateNumberProperty(assetId, propertyId, value).execute
    }
  }

  def deleteNumberPropertyValue(assetId: Long, propertyPublicId: String): Unit = {
    val propertyId = Q.query[String, Long](propertyIdByPublicId).apply(propertyPublicId).firstOption.getOrElse(throw new IllegalArgumentException("Property: " + propertyPublicId + " not found"))
    deleteNumberProperty(assetId, propertyId)
  }

  private def numberPropertyValueDoesNotExist(assetId: Long, propertyId: Long) = {
    Q.query[(Long, Long), Long](existsNumberProperty).apply((assetId, propertyId)).firstOption.isEmpty
  }

  private def textPropertyValueDoesNotExist(assetId: Long, propertyId: Long) = {
    Q.query[(Long, Long), Long](existsTextProperty).apply((assetId, propertyId)).firstOption.isEmpty
  }

  private def singleChoiceValueDoesNotExist(assetId: Long, propertyId: Long) = {
    Q.query[(Long, Long), Long](existsSingleChoiceProperty).apply((assetId, propertyId)).firstOption.isEmpty
  }

  private def updateCommonAssetProperty(assetId: Long, propertyPublicId: String, propertyType: String, propertyValues: Seq[PropertyValue]) {
    val property = AssetPropertyConfiguration.commonAssetProperties(propertyPublicId)
    propertyType match {
      case SingleChoice => {
        val newVal = propertyValues.head.propertyValue.toString
        AssetPropertyConfiguration.commonAssetPropertyEnumeratedValues.find { p =>
          (p.publicId == propertyPublicId) && (p.values.map(_.propertyValue).contains(newVal))
        } match {
          case Some(propValues) => {
            updateCommonProperty(assetId, property.column, newVal, property.lrmPositionProperty).execute
          }
          case None => throw new IllegalArgumentException("Invalid property/value: " + propertyPublicId + "/" + newVal)
        }
      }
      case Text | LongText => updateCommonProperty(assetId, property.column, propertyValues.head.propertyValue).execute
      case Date => {
        val formatter = ISODateTimeFormat.dateOptionalTimeParser()
        val optionalDateTime = propertyValues.headOption match {
          case None => None
          case Some(x) if x.propertyValue.trim.isEmpty => None
          case Some(x) => Some(formatter.parseDateTime(x.propertyValue))
        }
        updateCommonDateProperty(assetId, property.column, optionalDateTime, property.lrmPositionProperty).execute
      }
      case ReadOnlyText | ReadOnlyNumber => {
        logger.debug("Ignoring read only property in update: " + propertyPublicId)
      }
      case t: String => throw new UnsupportedOperationException("Asset: " + propertyPublicId + " property type: " + t + " not supported")
    }
  }

  private[this] def createOrUpdateMultipleChoiceProperty(propertyValues: Seq[PropertyValue], assetId: Long, propertyId: Long) {
    val newValues = propertyValues.map(_.propertyValue.toLong)
    val currentIdsAndValues = Q.query[(Long, Long), (Long, Long)](multipleChoicePropertyValuesByAssetIdAndPropertyId).apply(assetId, propertyId).list
    val currentValues = currentIdsAndValues.map(_._2)
    // remove values as necessary
    currentIdsAndValues.foreach {
      case (multipleChoiceId, enumValue) =>
        if (!newValues.contains(enumValue)) {
          deleteMultipleChoiceValue(multipleChoiceId).execute
        }
    }
    // add values as necessary
    newValues.filter {
      !currentValues.contains(_)
    }.foreach {
      v =>
        insertMultipleChoiceValue(assetId, propertyId, v).execute
    }
  }

  def getMunicipalityNameByCode(code: Int): String = {
    sql"""
      select name_fi from municipality where id = $code
    """.as[String].first
  }

  def propertyDefaultValues(assetTypeId: Long): List[SimpleProperty] = {
    implicit val getDefaultValue = new GetResult[SimpleProperty] {
      def apply(r: PositionedResult) = {
        SimpleProperty(publicId = r.nextString, values = List(PropertyValue(r.nextString)))
      }
    }
    sql"""
      select p.public_id, p.default_value from asset_type a
      join property p on p.asset_type_id = a.id
      where a.id = $assetTypeId and p.default_value is not null""".as[SimpleProperty].list
  }

  def getAssetAdministrationClass(assetId: Long): Option[AdministrativeClass] = {
    val propertyValueOption = getNumberPropertyValue(assetId, "linkin_hallinnollinen_luokka")

    propertyValueOption match {
      case None => None
      case Some(propertyValue) =>
        Some(AdministrativeClass.apply(propertyValue))
    }
  }

  def getAssetFloatingReason(assetId: Long): Option[FloatingReason] = {
    val propertyValueOption = getNumberPropertyValue(assetId, "kellumisen_syy")

    propertyValueOption match {
      case None => None
      case Some(propertyValue) =>
        Some(FloatingReason.apply(propertyValue))
    }
  }

  def expireMassTransitStop(username: String, id: Long) = {
    sqlu"""
             update asset
             set valid_to = sysdate -1, modified_date = sysdate, modified_by = $username
             where id = $id
          """.execute
  }

  //TODO: Fixme. Is distinct needed?
  def getPropertyDescription(propertyPublicId : String, value: String) = {
    sql"""
       Select distinct
         case
                 when e.name_fi is not null then e.name_fi
                 when tp.value_fi is not null then tp.value_fi
                 when np.value is not null then to_char(np.value)
                 else null
               end as display_value
       From PROPERTY p left join ENUMERATED_VALUE e on e.PROPERTY_ID = p.ID left join TEXT_PROPERTY_VALUE tp on
         tp.PROPERTY_ID = p.ID left join NUMBER_PROPERTY_VALUE np on np.PROPERTY_ID = p.ID
       Where p.PUBLIC_ID = $propertyPublicId And e.value = $value
      """.as[String].list
  }

  def deleteAllMassTransitStopData(assetId: Long): Unit ={
    sqlu"""Delete From Single_Choice_Value Where asset_id in (Select id as asset_id From asset Where id = $assetId)""".execute
    sqlu"""Delete From Multiple_Choice_Value Where asset_id in (Select id as asset_id From asset Where id = $assetId)""".execute
    sqlu"""Delete From Text_Property_Value Where asset_id in (Select id as asset_id From asset Where id = $assetId)""".execute
    sqlu"""Delete From Asset_Link Where asset_id in (Select id as asset_id From asset Where id = $assetId)""".execute
    sqlu"""Delete From Number_Property_Value Where asset_id in (Select id as asset_id From asset Where id = $assetId)""".execute
    sqlu"""Delete From Asset Where id = $assetId""".execute
  }

  def updateLrmPosition(id: Long, mValue: Double, linkId: Long) {
    sqlu"""
           update lrm_position
           set start_measure = $mValue, end_measure = $mValue, link_id = $linkId
           where id = (
            select lrm.id
            from asset a
            join asset_link al on al.asset_id = a.id
            join lrm_position lrm on lrm.id = al.position_id
            where a.id = $id)
      """.execute
  }

  def insertLrmPosition(id: Long, mValue: Double, linkId: Long) {
    sqlu"""
           insert into lrm_position (id, start_measure, end_measure, link_id)
           values ($id, $mValue, $mValue, $linkId)
      """.execute
  }

  def insertAsset(id: Long, nationalId: Long, lon: Double, lat: Double, bearing: Int, creator: String, municipalityCode: Int, floating: Boolean): Unit = {
    val typeId = 10
    sqlu"""
           insert into asset (id, external_id, asset_type_id, bearing, created_by, municipality_code, geometry, floating)
           values ($id, $nationalId, $typeId, $bearing, $creator, $municipalityCode,
           MDSYS.SDO_GEOMETRY(4401, 3067, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1,1,1), MDSYS.SDO_ORDINATE_ARRAY($lon, $lat, 0, 0)),
           $floating)
      """.execute
  }

  def insertAssetLink(assetId: Long, lrmPositionId: Long): Unit = {

    sqlu"""
           insert into asset_link(asset_id, position_id)
           values ($assetId, $lrmPositionId)
      """.execute
  }

  def updateBearing(id: Long, position: Position) {
    position.bearing.foreach { bearing =>
      sqlu"""
           update asset
           set bearing = $bearing
           where id = $id
        """.execute
    }
  }

  def updateMunicipality(id: Long, municipalityCode: Int) {
    sqlu"""
           update asset
           set municipality_code = $municipalityCode
           where id = $id
      """.execute
  }

}
