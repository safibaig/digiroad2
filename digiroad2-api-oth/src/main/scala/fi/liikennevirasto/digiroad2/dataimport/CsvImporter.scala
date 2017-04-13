package fi.liikennevirasto.digiroad2.dataimport

import java.io.{InputStreamReader, InputStream}
import com.github.tototoshi.csv._
import fi.liikennevirasto.digiroad2.Digiroad2Context._
import fi.liikennevirasto.digiroad2.dataimport.CsvImporter._
import fi.liikennevirasto.digiroad2.user.UserProvider
import fi.liikennevirasto.digiroad2._
import fi.liikennevirasto.digiroad2.masstransitstop.oracle.MassTransitStopDao
import org.apache.commons.lang3.StringUtils.isBlank
import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.masstransitstop.MassTransitStopOperations

object CsvImporter {
  case class NonExistingAsset(externalId: Long, csvRow: String)
  case class IncompleteAsset(missingParameters: List[String], csvRow: String)
  case class MalformedAsset(malformedParameters: List[String], csvRow: String)
  case class ExcludedAsset(affectedRoadLinkType: String, csvRow: String)
  case class ImportResult(nonExistingAssets: List[NonExistingAsset] = Nil,
                          incompleteAssets: List[IncompleteAsset] = Nil,
                          malformedAssets: List[MalformedAsset] = Nil,
                          excludedAssets: List[ExcludedAsset] = Nil)
}

class AssetNotFoundException(externalId: Long) extends RuntimeException

trait CsvImporter {
  val massTransitStopService: MassTransitStopService
  val vvhClient: VVHClient
  val userProvider: UserProvider

  case class CsvAssetRow(externalId: Long, properties: Seq[SimpleProperty])

  type MalformedParameters = List[String]
  type ParsedProperties = List[SimpleProperty]
  type ParsedAssetRow = (MalformedParameters, ParsedProperties)
  type ExcludedRoadLinkTypes = List[AdministrativeClass]

  private def maybeInt(string: String): Option[Int] = {
    try {
      Some(string.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  private val isValidTypeEnumeration = Set(1, 2, 3, 4, 5, 99)

  private val singleChoiceValueMappings = Set(1, 2, 99).map(_.toString)

  private val stopAdministratorProperty = "tietojen_yllapitaja";

  private val stopAdministratorValueMappings = Set(1, 2, 3, 99).map(_.toString)

  private val textFieldMappings = Map(
    "Pysäkin nimi" -> "nimi_suomeksi" ,
    "Ylläpitäjän tunnus" -> "yllapitajan_tunnus",
    "LiVi-tunnus" -> "yllapitajan_koodi",
    "Matkustajatunnus" -> "matkustajatunnus",
    "Pysäkin nimi ruotsiksi" -> "nimi_ruotsiksi",
    "Liikennöintisuunta" -> "liikennointisuunta",
    "Lisätiedot" -> "lisatiedot"
  )

  private val multipleChoiceFieldMappings = Map(
    "Pysäkin tyyppi" -> "pysakin_tyyppi"
  )

  private val singleChoiceFieldMappings = Map(
    "Aikataulu" -> "aikataulu",
    "Katos" -> "katos",
    "Mainoskatos" -> "mainoskatos",
    "Penkki" -> "penkki",
    "Pyöräteline" -> "pyorateline",
    "Sähköinen aikataulunäyttö" -> "sahkoinen_aikataulunaytto",
    "Valaistus" -> "valaistus",
    "Saattomahdollisuus henkilöautolla" -> "saattomahdollisuus_henkiloautolla",
    "Tietojen ylläpitäjä" -> stopAdministratorProperty
  )

  val mappings = textFieldMappings ++ multipleChoiceFieldMappings ++ singleChoiceFieldMappings

  val MandatoryParameters: Set[String] = mappings.keySet + "Valtakunnallinen ID"


  private def resultWithType(result: (MalformedParameters, List[SimpleProperty]), assetType: Int): ParsedAssetRow = {
    result.copy(_2 = result._2 match {
      case List(SimpleProperty("pysakin_tyyppi", xs)) => List(SimpleProperty("pysakin_tyyppi", PropertyValue(assetType.toString) :: xs.toList))
      case _ => List(SimpleProperty("pysakin_tyyppi", Seq(PropertyValue(assetType.toString))))
    })
  }

  private def assetTypeToProperty(assetTypes: String): ParsedAssetRow = {
    val invalidAssetType = (List("Pysäkin tyyppi"), Nil)
    val types = assetTypes.split(',')
    if(types.isEmpty) invalidAssetType
    else {
      types.foldLeft((Nil: MalformedParameters, Nil: ParsedProperties)) { (result, assetType) =>
        maybeInt(assetType.trim)
          .filter(isValidTypeEnumeration)
          .map(typeEnumeration => resultWithType(result, typeEnumeration))
          .getOrElse(invalidAssetType)
      }
    }
  }

  private def assetSingleChoiceToProperty(parameterName: String, assetSingleChoice: String): ParsedAssetRow = {
    // less than ideal design but the simplest solution. DO NOT REPEAT IF MORE FIELDS REQUIRE CUSTOM VALUE VALIDATION
    val isValidStopAdminstratorValue = singleChoiceFieldMappings(parameterName) == stopAdministratorProperty && stopAdministratorValueMappings(assetSingleChoice)

    if (singleChoiceValueMappings(assetSingleChoice) || isValidStopAdminstratorValue) {
      (Nil, List(SimpleProperty(singleChoiceFieldMappings(parameterName), List(PropertyValue(assetSingleChoice)))))
    } else {
      (List(parameterName), Nil)
    }
  }

  private def assetRowToProperties(csvRowWithHeaders: Map[String, String]): ParsedAssetRow = {
    csvRowWithHeaders.foldLeft((Nil: MalformedParameters, Nil: ParsedProperties)) { (result, parameter) =>
      val (key, value) = parameter
      if(isBlank(value)) {
        result
      } else {
        if (textFieldMappings.contains(key)) {
          result.copy(_2 = SimpleProperty(publicId = textFieldMappings(key), values = Seq(PropertyValue(value))) :: result._2)
        } else if (multipleChoiceFieldMappings.contains(key)) {
          val (malformedParameters, properties) = assetTypeToProperty(value)
          result.copy(_1 = malformedParameters ::: result._1, _2 = properties ::: result._2)
        } else if (singleChoiceFieldMappings.contains(key)) {
          val (malformedParameters, properties) = assetSingleChoiceToProperty(key, value)
          result.copy(_1 = malformedParameters ::: result._1, _2 = properties ::: result._2)
        } else {
          result
        }
      }
    }
  }

  private def municipalityValidation(municipality: Int): Unit = {
    if (!userProvider.getCurrentUser().isAuthorizedToWrite(municipality)) {
      throw new IllegalArgumentException("User does not have write access to municipality")
    }
  }

  private def updateAssetByExternalId(externalId: Long, properties: Seq[SimpleProperty]): MassTransitStopWithProperties = {
    val optionalAsset = massTransitStopService.getMassTransitStopByNationalId(externalId, municipalityValidation)
    optionalAsset match {
      case Some(asset) =>
        val verifiedProperties = getVerifiedProperties(properties.toSet, asset.propertyData)
        massTransitStopService.updateExistingById(asset.id, None, verifiedProperties, userProvider.getCurrentUser().username, () => _)
      case None => throw new AssetNotFoundException(externalId)
    }
  }

  private def updateAssetByExternalIdLimitedByRoadType(externalId: Long, properties: Seq[SimpleProperty], roadTypeLimitations: Set[AdministrativeClass]): Either[AdministrativeClass, MassTransitStopWithProperties] = {
    class CsvImportMassTransitStop(val id: Long, val floating: Boolean, val roadLinkType: AdministrativeClass) extends FloatingAsset {}
    def massTransitStopTransformation(stop: PersistedMassTransitStop): (CsvImportMassTransitStop, Option[FloatingReason]) = {
      val roadLink = vvhClient.fetchByLinkId(stop.linkId)
      val (floating, floatingReason) = massTransitStopService.isFloating(stop, roadLink)
      (new CsvImportMassTransitStop(stop.id, floating, roadLink.map(_.administrativeClass).getOrElse(Unknown)), floatingReason)
    }

    val optionalAsset = massTransitStopService.getByNationalId(externalId, municipalityValidation, massTransitStopTransformation)
    optionalAsset match {
      case Some(asset) =>
        val roadLinkType = asset.roadLinkType
        val verifiedProperties = getVerifiedPropertiesByNationalId(properties.toSet, asset.id)
        if (roadTypeLimitations(roadLinkType)) Right(massTransitStopService.updateExistingById(asset.id, None, verifiedProperties, userProvider.getCurrentUser().username, () => _))
        else Left(roadLinkType)
      case None => throw new AssetNotFoundException(externalId)
    }
  }

  def rowToString(csvRowWithHeaders: Map[String, Any]): String = {
    csvRowWithHeaders.view map { case (key, value) => key + ": '" + value + "'"} mkString ", "
  }

  private def findMissingParameters(csvRowWithHeaders: Map[String, String]): List[String] = {
    MandatoryParameters.diff(csvRowWithHeaders.keys.toSet).toList
  }

  def updateAsset(externalId: Long, properties: Seq[SimpleProperty], roadTypeLimitations: Set[AdministrativeClass]): ExcludedRoadLinkTypes = {
    if(roadTypeLimitations.nonEmpty) {
      val result: Either[AdministrativeClass, MassTransitStopWithProperties] = updateAssetByExternalIdLimitedByRoadType(externalId, properties, roadTypeLimitations)
      result match {
        case Left(roadLinkType) => List(roadLinkType)
        case _ => Nil
      }
    } else {
      updateAssetByExternalId(externalId, properties)
      Nil
    }
  }

  def importAssets(inputStream: InputStream, roadTypeLimitations: Set[AdministrativeClass] = Set()): ImportResult = {
    val streamReader = new InputStreamReader(inputStream, "UTF-8")
    val csvReader = CSVReader.open(streamReader)(new DefaultCSVFormat {
      override val delimiter: Char = ';'
    })
    csvReader.allWithHeaders().foldLeft(ImportResult()) { (result, row) =>
      val missingParameters = findMissingParameters(row)
      val (malformedParameters, properties) = assetRowToProperties(row)
      if(missingParameters.isEmpty && malformedParameters.isEmpty) {
        val parsedRow = CsvAssetRow(externalId = row("Valtakunnallinen ID").toLong, properties = properties)
        try {
          val excludedAssets = updateAsset(parsedRow.externalId, parsedRow.properties, roadTypeLimitations)
            .map(excludedRoadLinkType => ExcludedAsset(affectedRoadLinkType = excludedRoadLinkType.toString, csvRow = rowToString(row)))
          result.copy(excludedAssets = excludedAssets ::: result.excludedAssets)
        } catch {
          case e: AssetNotFoundException => result.copy(nonExistingAssets = NonExistingAsset(externalId = parsedRow.externalId, csvRow = rowToString(row)) :: result.nonExistingAssets)
        }
      } else {
        result.copy(
          incompleteAssets = missingParameters match {
            case Nil => result.incompleteAssets
            case parameters => IncompleteAsset(missingParameters = parameters, csvRow = rowToString(row)) :: result.incompleteAssets
          },
          malformedAssets = malformedParameters match {
            case Nil => result.malformedAssets
            case parameters => MalformedAsset(malformedParameters = parameters, csvRow = rowToString(row)) :: result.malformedAssets
          }
        )
      }
    }
  }

  /**
    * This method fetches Tietojen ylläpitäjä value from old asset, if value is empty in csv, to prevent clearing LiviId with unknown value.
    * @param properties
    * @param assetProperties
    * @return
    */
  private def getVerifiedProperties(properties: Set[SimpleProperty], assetProperties: Seq[AbstractProperty]): Set[SimpleProperty] = {
    val administratorInfoFromProperties = properties.find(_.publicId == MassTransitStopOperations.AdministratorInfoPublicId)

    administratorInfoFromProperties.flatMap(_.values.headOption.map(_.propertyValue)) match {
      case Some(value) => properties
      case None =>
        val administratorInfoFromAsset = assetProperties.find(_.publicId == MassTransitStopOperations.AdministratorInfoPublicId).flatMap(prop => prop.values.headOption).get.propertyValue
        val oldAdministratorInfoProperty = Seq(SimpleProperty(MassTransitStopOperations.AdministratorInfoPublicId, Seq(PropertyValue(administratorInfoFromAsset))))
        properties ++ oldAdministratorInfoProperty
    }
  }

  private def getVerifiedPropertiesByNationalId(properties: Set[SimpleProperty], externalId: Long): Set[SimpleProperty] = {
    val optionalAsset = massTransitStopService.getMassTransitStopByNationalId(externalId, municipalityValidation)
    optionalAsset match {
      case Some(asset) =>
        getVerifiedProperties(properties.toSet, asset.propertyData)
      case None => throw new AssetNotFoundException(externalId)
    }
  }

}
