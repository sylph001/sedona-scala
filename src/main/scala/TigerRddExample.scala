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
import com.amazonaws.services.logs.model.QueryInfo
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

/**
 1. select edges.id, arealm.id from edges join arealm on ST_INTERSECTS(edges.geom, arealm.geom);
 2. select arealm.id, areawater.id from arealm join areawater on ST_TOUCHES(arealm.geom, areawater.geom);
 3. select edges.id, arealm.id from edges join arealm on ST_CROSSES(edges.geom, arealm.geom);
 4. select edges.id, edges2.id from edges join edges as edges on ST_CROSSES(edges.geom, edges2.geom);
 5. select edges.id, areawater.id from edges join areawater on ST_CROSSES(edges.geom, areawater.geom);
 6. select areawater.id, areawater2.id from areawater join areawater as areawater2 on ST_OVERLAPS(areawater.geom, areawater2.geom);
 7. select arealm.id, arealm2.id from arealm join arealm as arealm2 on ST_OVERLAPS(arealm.geom, arealm2.geom);
 8. select pointlm.id, areawater.id from pointlm join areawater on ST_WITHIN(pointlm.geom, areawater.geom);
 9. Distance Join:
    select arealmp.id, pointlmp.id from arealmp, pointlmp where ST_DISTANCE(arealmp.geom, pointlmp.geom) <= 1
10. Range Join:
    select arealmp.id, pointlmp.id from arealmp, pointlmp where ST_DWITHIN(arealmp.geom, pointlmp.geom, 1)
 */

object TigerRddExample {

  //val arealmFileLocation = resourceFolder+"tiger/arealm"
  //val arealWaterFileLocation = resourceFolder+"tiger/areawater"
  val arealmFileLocation = "hdfs://10.32.0.1:/home/sedona/tiger/shp/arealm"
  val arealWaterFileLocation = "hdfs://10.32.0.1:/home/sedona/tiger/shp/areawater"
  val edgesFileLocation = "hdfs://10.32.0.1:/home/sedona/tiger/shp/edges"
  val pointlmFileLocation = "hdfs://10.32.0.1:/home/sedona/tiger/shp/pointlm"

  val mapQueries = Map( //:Map[Int, List[String]] = Map[Int, List[String]]()
    1 -> List(edgesFileLocation, arealmFileLocation, "INTERSECTS"),
    2 -> List(arealmFileLocation, arealWaterFileLocation, "TOUCHES")
    3 -> List(edgesFileLocation, arealmFileLocation, "CROSSES"),
    4 -> List(edgesFileLocation, edgesFileLocation, "CROSSES"),
    5 -> List(edgesFileLocation, arealWaterFileLocation, "CROSSES")
  )

  val mapPredicates = Map( //:Map[String, SpatialPredicate] = Map[String, SpatialPredicate]()
    "INTERSECTS" -> SpatialPredicate.INTERSECTS,
    "TOUCHES" -> SpatialPredicate.TOUCHES,
    "CROSSES" -> SpatialPredicate.CROSSES
  )

  def runTigerQuery(sedona: SparkSession, QueryInfo: List[String]): Unit =
  {
    var buildRDD = new SpatialRDD[Geometry]()
    var probeRDD = new SpatialRDD[Geometry]()

    buildRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, QueryInfo.head)
    probeRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, QueryInfo(1))

    buildRDD.analyze()
    buildRDD.spatialPartitioning(GridType.KDBTREE)

    probeRDD.analyze()
    probeRDD.spatialPartitioning(buildRDD.getPartitioner)

    val switchBuildOnPartition = true
    val switchUseIndex = true

    val startIndexSetting = System.currentTimeMillis()
    buildRDD.buildIndex(IndexType.QUADTREE, switchBuildOnPartition)
    buildRDD.indexedRDD = buildRDD.indexedRDD.cache()
    val endIndexSetting = System.currentTimeMillis()

    val res = JoinQuery.SpatialJoinQuery(buildRDD, probeRDD, switchUseIndex, mapPredicates(QueryInfo(2)))
    println("RUN $cnt result: ")
    println(res.count())
  }

  def runTigerQueryOrig(sedona: SparkSession): Unit =
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

