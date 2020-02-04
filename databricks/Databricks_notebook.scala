// Databricks notebook source
import com.google.gson.{JsonArray, JsonObject, JsonParser}
import java.awt.Color
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Row}

// COMMAND ----------

def matchToRange(value: Float): Float = value match {
  case value if value < 0.0f => 0.0f
  case value if value > 1.0f => 1.0f
  case _ => value
}

// COMMAND ----------

def convertToRGB(p: Float, q: Float, h: Float):Float = {
  val h2 = if (h < 0) h + 1 else if (h > 1) h - 1 else h

  if (6.0f * h2 < 1) {
    p + ((q - p) * 6.0f * h2)
  } else if (2.0f * h2 < 1){
    q
  } else if (3.0f * h2 < 2) {
    p + ( (q - p) * 6.0f * ((2.0f / 3.0f) - h2) )
  } else {
    p
  }
}

// COMMAND ----------

def HSLAtoRGBA(hue:Float, saturation:Float, luminance:Float, alpha:Float):Color = {
  if (hue <0.0f || hue > 360.0f)
  {
    throw new IllegalArgumentException("Hue outside of expected range (0-360)" )
  }
  if (saturation <0.0f || saturation > 100.0f)
  {
    throw new IllegalArgumentException("Saturation outside of expected range (0-100)" )
  }

  if (luminance <0.0f || luminance > 100.0f)
  {
    throw new IllegalArgumentException("Luminance outside of expected range (0-100)" )
  }

  if (alpha <0.0f || alpha > 100.0f)
  {
    throw new IllegalArgumentException("Alpha outside of expected range (0-1)" )
  }

  val hueN: Float = (hue % 360.0f)/360f
  val saturationN = saturation / 100.0f
  val luminanceN = luminance / 100.0f
  val alphaN = alpha / 100.0f

  val q: Float = if (luminanceN < 0.5) luminanceN * (1 + saturationN) else (luminanceN + saturationN) - (saturationN * luminanceN)
  val p: Float = 2 * luminanceN - q

  val redN: Float = matchToRange(convertToRGB(p, q, hueN + (1.0f / 3.0f)))
  val greenN: Float = matchToRange(convertToRGB(p, q, hueN))
  val blueN: Float = matchToRange(convertToRGB(p, q, hueN - (1.0f / 3.0f)))

  new Color(redN, greenN, blueN, alphaN)
}

// COMMAND ----------

def setColor(value:Int, min:Int, max:Int): String = {
  val level: Double = 1 - (value - min).toDouble/(max - min).toDouble
  val hue: Float = level.toFloat * 180.0f

  val color: Color = HSLAtoRGBA(hue, 100.0f, 50.0f, 100.0f)

  s"rgba(${color.getRed}, ${color.getGreen}, ${color.getBlue}, ${color.getAlpha})"
}

// COMMAND ----------

case class Coordinates(lng: Double, lat: Double)

// COMMAND ----------

def generatePolygon(coordinates: Coordinates, color:String, value:Int): JsonObject = {
  def generateGeometry(coordinates: Coordinates): JsonObject = {
    val borderPoints = new JsonArray()

    val parser = new JsonParser()

    val NWpoint = new JsonArray()
    NWpoint.add(parser.parse((coordinates.lng - 0.005).toString))
    NWpoint.add(parser.parse((coordinates.lat + 0.005).toString))

    val NEpoint = new JsonArray()
    NEpoint.add(parser.parse((coordinates.lng + 0.005).toString))
    NEpoint.add(parser.parse((coordinates.lat + 0.005).toString))

    val SEpoint = new JsonArray()
    SEpoint.add(parser.parse((coordinates.lng + 0.005).toString))
    SEpoint.add(parser.parse((coordinates.lat - 0.005).toString))

    val SWpoint = new JsonArray()
    SWpoint.add(parser.parse((coordinates.lng - 0.005).toString))
    SWpoint.add(parser.parse((coordinates.lat - 0.005).toString))

    val points = new JsonArray()
    points.add(NWpoint)
    points.add(NEpoint)
    points.add(SEpoint)
    points.add(SWpoint)
    points.add(NWpoint)

    borderPoints.add(points)

    val geometry = new JsonObject
    geometry.addProperty("type", "Polygon")
    geometry.add("coordinates", borderPoints)

    geometry
  }

  val properties = new JsonObject
  properties.addProperty("value", value)
  properties.addProperty("fill", color)
  properties.addProperty("fill-opacity", 0.5)
  properties.addProperty("stroke-width", 0)

  val feature = new JsonObject
  feature.addProperty("type", "Feature")
  feature.add("geometry", generateGeometry(coordinates))
  feature.add("properties", properties)
  feature
}

// COMMAND ----------

def round(value: Float, places: Int): Double = {
  BigDecimal(value.toString).setScale(places, BigDecimal.RoundingMode.HALF_UP).toDouble
}

// COMMAND ----------

def generateFeatures(rectanglesDF:DataFrame): JsonArray = {
  val minMax: Row = rectanglesDF.agg(org.apache.spark.sql.functions.min("count"), org.apache.spark.sql.functions.max("count")).head()

  var features = new JsonArray()
  // TODO: find another JSON library for work with serialization
  rectanglesDF.collect.foreach(item => {
    val coordinates: Coordinates = new Coordinates(round(item.getFloat(0), 2), round(item.getFloat(1), 2))
    val value: Int = item.getLong(2).toInt
    val color: String = setColor(value, minMax.getLong(0).toInt, minMax.getLong(1).toInt)

    features.add(generatePolygon(coordinates, color, value))
  })
  features
}

// COMMAND ----------

def convertToRectanglesHeatmap(rectanglesDF:DataFrame): JsonObject = {
  val gsonTrajectory = new JsonObject
  gsonTrajectory.addProperty("type", "FeatureCollection")
  gsonTrajectory.add("features", generateFeatures(rectanglesDF:DataFrame))

  gsonTrajectory
}

// COMMAND ----------

def saveToFile(gsonTrajectory: JsonObject, fileName: String): Unit = {
  import com.microsoft.azure.storage._
  import com.microsoft.azure.storage.blob._

  val connectStr = "DefaultEndpointsProtocol=https;AccountName=karolpteststorage;AccountKey=<account_key>;EndpointSuffix=core.windows.net"
  val storageAccount = CloudStorageAccount.parse(connectStr)
  val blobClient = storageAccount.createCloudBlobClient()
  val container = blobClient.getContainerReference("heatmap-geojson");
  val blob = container.getBlockBlobReference(fileName);

  blob.uploadText(gsonTrajectory.toString)
}

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.karolpteststorage.blob.core.windows.net",
  <your_key>)

val trafficVolume = spark.read.parquet("wasbs://traffic-volume@karolpteststorage.blob.core.windows.net/")

val trafficVolumeGeoJSON = convertToRectanglesHeatmap(trafficVolume)

saveToFile(trafficVolumeGeoJSON,"rectangles_heatmap_2.json")
