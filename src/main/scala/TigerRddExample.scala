/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Main.resourceFolder
import com.amazonaws.thirdparty.joda.time.format.PeriodFormat
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.{CircleRDD, SpatialRDD}
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.viz.core.{ImageGenerator, RasterOverlayOperator}
import org.apache.sedona.viz.extension.visualizationEffect.{HeatMap, ScatterPlot}
import org.apache.sedona.viz.utils.ImageType
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import java.awt.Color

object TigerRddExample {

  val arealmFileLocation = resourceFolder+"tiger/arealm"
  val arealWaterFileLocation = resourceFolder+"tiger/areawater"

  def runTigerQuery(sedona: SparkSession): Unit =
  {

    // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
    var arealmRDD = new SpatialRDD[Geometry]()
    var areaWaterRDD = new SpatialRDD[Geometry]()

    arealmRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, arealmFileLocation)
    areaWaterRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, arealWaterFileLocation)

    val spatialPredicate = SpatialPredicate.TOUCHES // Only return gemeotries fully covered by each query window in queryWindowRDD
    arealmRDD.analyze()
    arealmRDD.spatialPartitioning(GridType.KDBTREE)
    //println("Index Build Time:")
    //+println(elapsed.toPeriod.toString(PeriodFormat.getDefault)

    areaWaterRDD.analyze()
    areaWaterRDD.spatialPartitioning(arealmRDD.getPartitioner)

    val indexStartTime = System.currentTimeMillis()
    println(s"Start: $indexStartTime")
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true
    areaWaterRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    areaWaterRDD.indexedRDD = areaWaterRDD.indexedRDD.cache()
    val indexEndTime = System.currentTimeMillis()
    println(s"Start: $indexEndTime")
    val indexTime = indexEndTime - indexStartTime
    println("Index build time: "+indexTime)

    val queryStart = System.currentTimeMillis()
    val result = JoinQuery.SpatialJoinQuery(arealmRDD, areaWaterRDD, usingIndex, spatialPredicate)
    System.out.println(result.count())
    val queryEnd = System.currentTimeMillis()
    val queryTime = queryEnd - queryStart
    println(s"Run1 $queryTime")

    val start2 = System.currentTimeMillis()
    val res2 = JoinQuery.SpatialJoinQuery(arealmRDD, areaWaterRDD, usingIndex, spatialPredicate)
    System.out.println(result.count())
    val end2 = System.currentTimeMillis()
    val querytime2 = end2 - start2
    println(s"Run2 $querytime2")

    val start3 = System.currentTimeMillis()
    val res3 = JoinQuery.SpatialJoinQuery(arealmRDD, areaWaterRDD, usingIndex, spatialPredicate)
    System.out.println(result.count())
    val end3 = System.currentTimeMillis()
    val querytime3 = end3 - start3
    println(s"Run3 $querytime3")
  }
}

