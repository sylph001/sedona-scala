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

//import Main.resourceFolder
//import TigerRddExample.areawaterFileLocation

import scala.collection.mutable.ListBuffer
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

  // Tiger Dataset
  //val arealmFileLocation = resourceFolder+"tiger/arealm"
  //val areaWaterFileLocation = resourceFolder+"tiger/areawater"
  val arealmFileLocation = "hdfs://9fcebbf32068:/home/sedona/tiger/shp/arealm"
  val areaWaterFileLocation = "hdfs://9fcebbf32068:/home/sedona/tiger/shp/areawater"
  val edgesFileLocation = "hdfs://9fcebbf32068:/home/sedona/tiger/shp/edges"
  val pointlmFileLocation = "hdfs://9fcebbf32068:/home/sedona/tiger/shp/pointlm"
  // OSM Dataset
  val bld_poly_uk = "hdfs://9fcebbf32068:/home/sedona/osm/shp/bld_poly_uk"
  val lwn_poly_uk = "hdfs://9fcebbf32068:/home/sedona/osm/shp/lwn_poly_uk"
  val poi_point_uk = "hdfs://9fcebbf32068:/home/sedona/osm/shp/poi_point_uk"
  val rds_lin_uk = "hdfs://9fcebbf32068:/home/sedona/osm/shp/rds_lin_uk"

  val mapQueries = Map( //:Map[Int, List[String]] = Map[Int, List[String]]()
    // Tiger
    1 -> List(edgesFileLocation, arealmFileLocation, "INTERSECTS"),
    2 -> List(arealmFileLocation, areaWaterFileLocation, "TOUCHES"),
    3 -> List(edgesFileLocation, arealmFileLocation, "CROSSES"),
    4 -> List(edgesFileLocation, edgesFileLocation, "CROSSES"),
    5 -> List(edgesFileLocation, areaWaterFileLocation, "CROSSES"),
    6 -> List(areaWaterFileLocation, areaWaterFileLocation, "OVERLAPS"),
    7 -> List(arealmFileLocation, arealmFileLocation, "OVERLAPS"),
    8 -> List(pointlmFileLocation, areaWaterFileLocation, "WITHIN"),
    // OSM
    9 -> List(rds_lin_uk, bld_poly_uk, "TOUCHES"),
    10 -> List(rds_lin_uk, lwn_poly_uk, "CROSSES"),
    11 -> List(poi_point_uk, lwn_poly_uk, "WITHIN"),
    12 -> List(bld_poly_uk, lwn_poly_uk, "OVERLAPS")
  )

  val mapPredicates = Map( //:Map[String, SpatialPredicate] = Map[String, SpatialPredicate]()
    "INTERSECTS" -> SpatialPredicate.INTERSECTS,
    "TOUCHES" -> SpatialPredicate.TOUCHES,
    "CROSSES" -> SpatialPredicate.CROSSES,
    "OVERLAPS" -> SpatialPredicate.OVERLAPS,
    "WITHIN" -> SpatialPredicate.WITHIN
    //"DISTANCE" -> SpatialPredicate.DISTANCE,
    //"DWITHIN" -> SpatialPredicate.DWITHIN
  )

  def runTigerQuery(sedona: SparkSession, QueryInfo: List[String], HotRunTimes: Int): Unit =
  {
    var buildRDD = new SpatialRDD[Geometry]()
    var probeRDD = new SpatialRDD[Geometry]()

    buildRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, QueryInfo.head)
    probeRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, QueryInfo(1))

    buildRDD.analyze()
    buildRDD.spatialPartitioning(GridType.QUADTREE)

    probeRDD.analyze()
    probeRDD.spatialPartitioning(buildRDD.getPartitioner)

    val switchBuildOnPartition = true
    val switchUseIndex = true

    val startIndexSetting = System.currentTimeMillis()
    buildRDD.buildIndex(IndexType.QUADTREE, switchBuildOnPartition)
    buildRDD.indexedRDD = buildRDD.indexedRDD.cache()
    val endIndexSetting = System.currentTimeMillis()
    val timeIndexSetting = endIndexSetting - startIndexSetting

    // Cold Run
    val startCold = System.currentTimeMillis()
    val resCold = JoinQuery.SpatialJoinQuery(buildRDD, probeRDD, switchUseIndex, mapPredicates(QueryInfo(2)))
    val resultCold = resCold.count()
    val endCold = System.currentTimeMillis()
    val timeCold= endCold - startCold

    // Hod Run
    val listTimeHotRun = new ListBuffer[Long]()
    val listResultHotRun = new ListBuffer[Long]()
    val sumTimeHotRun = 0.0
    for (n <- List.range(0, HotRunTimes)) {
      val startHot = System.currentTimeMillis()
      println(s"Run $n: Hot Start $startHot")

      // Query
      val resHot = JoinQuery.SpatialJoinQuery(buildRDD, probeRDD, switchUseIndex, mapPredicates(QueryInfo(2)))
      listResultHotRun += resHot.count()

      val endHot = System.currentTimeMillis()
      println(s"Run $n: Hot End $endHot")
      val timeHot= endHot - startHot
      listTimeHotRun += timeHot
      //sumTimeHotRun = sumTimeHotRun + timeHot
    }
    val sumTime = listTimeHotRun.sum
    val avgHotTime = sumTime / HotRunTimes


    val strTag = "$$RESULT$$  "
    println(s"$strTag  User Defined Hot Run Times: $HotRunTimes\n")
    println(s"$strTag  IndexSetting Time: ${timeIndexSetting}")
    println(s"$strTag  Cold Run Result: ${resultCold}")
    println(s"$strTag  Cold Run Time: ${timeCold}")

    println(s"")
    for (n <- List.range(0, HotRunTimes)) {
      println(s"$strTag  RUN $n Result: ${listResultHotRun(n)}")
      println(s"$strTag  RUN $n Time: ${listTimeHotRun(n)}")
    }
    println(s"")
    println(s"$strTag  Avg Hot Run Time: $avgHotTime")
  }

  val mapQuerySQL = Map( //:Map[Int, List[String]] = Map[Int, List[String]]()
    // Spatial Join
    // 1 -> List(edgesFileLocation, arealmFileLocation, "INTERSECTS"),
    2 -> List(areaWaterFileLocation, arealmFileLocation, "y", "areawater", "arealm", "select areawater.id, arealm.id from areawater join arealm on ST_TOUCHES(areawater.geometry, arealm.geometry)"),
    3 -> List(edgesFileLocation, arealmFileLocation, "y", "edges", "arealm", "select edges.id, arealm.id from edges join arealm on ST_CROSSES(edges.geometry, arealm.geometry)"),
    4 -> List(edgesFileLocation, edgesFileLocation, "y", "edges", "edges2", "select edges.id, edges2.id from edges join edges as edges2 on ST_CROSSES(edges.geometry, edges2.geometry)"),
    5 -> List(edgesFileLocation, areaWaterFileLocation, "y", "edges", "areawater", "select edges.id, areawater.id from edges join areawater on ST_CROSSES(edges.geometry, areawater.geometry)"),
    6 -> List(areaWaterFileLocation, areaWaterFileLocation, "y", "areawater", "areawater2", "select areawater.id, areawater2.id from areawater join areawater as areawater2 on ST_OVERLAPS(areawater.geometry, areawater2.geometry)"),
    // 7 -> List(arealmFileLocation, arealmFileLocation, "OVERLAPS"),
    8 -> List(areaWaterFileLocation, pointlmFileLocation, "select areawater.id pointlm.id, from areawater join pointlm on ST_WITHIN(areawater.geometry, pointlm.geometry)"),
    // Range
    9 -> List(edgesFileLocation, arealmFileLocation, "y", "edges", "arealm", "select 1 from edges join arealm on ST_DISTANCE(edges.geometry, arealm.geometry)"),
    10 -> List(areaWaterFileLocation, pointlmFileLocation, "y", "areawater", "pointlm", "select 1 from areawater join pointlm on ST_DWITHIN(areawater.geometry, pointlm.geometry, 1)"),
    // OSM
    11 -> List(bld_poly_uk, rds_lin_uk, "y", "bld_poly_uk", "rds_lin_uk", "select * from bld_poly_uk join rds_lin_uk on ST_TOUCHES(bld_poly_uk.geometry, rds_lin_uk.geometry)"),
    12 -> List(lwn_poly_uk, rds_lin_uk, "y", "lwn_poly_uk", "rds_lin_uk", "select * from lwn_poly_uk join rds_lin_uk on ST_CROSSES(lwn_poly_uk.geometry, rds_lin_uk.geometry)"),
    13 -> List(lwn_poly_uk, poi_point_uk, "y", "lwn_poly_uk", "poi_point_uk", "select * from lwn_poly_uk join poi_point_uk on ST_WITHIN(lwn_poly_uk.geometry, poi_point_uk.geometry)"),
    14 -> List(bld_poly_uk, lwn_poly_uk, "y", "bld_poly_uk", "lwn_poly_uk", "select * from bld_poly_uk join lwn_poly_uk on ST_OVERLAPS(bld_poly_uk.geometry, lwn_poly_uk.geometry)")
  )

  def runQuerySQLAPI(sedona: SparkSession, QueryInfo: List[String], HotRunTimes: Int): Unit ={
    var buildRDD = new SpatialRDD[Geometry]()
    var probeRDD = new SpatialRDD[Geometry]()

    probeRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, QueryInfo.head)
    buildRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, QueryInfo(1))

    //buildRDD.analyze()
    //buildRDD.spatialPartitioning(GridType.QUADTREE)

    probeRDD.analyze()
    probeRDD.spatialPartitioning(GridType.QUADTREE) //buildRDD.getPartitioner)

    val switchBuildOnPartition = true
    var switchUseIndex = true
    if (QueryInfo(2) != "y") {
      switchUseIndex = false
    }

    val startIndexSetting = System.currentTimeMillis()
    //buildRDD.buildIndex(IndexType.QUADTREE, switchBuildOnPartition)
    //buildRDD.indexedRDD = buildRDD.indexedRDD.cache()
    probeRDD.buildIndex(IndexType.QUADTREE, switchBuildOnPartition)
    probeRDD.indexedRDD = probeRDD.indexedRDD.cache()
    val endIndexSetting = System.currentTimeMillis()
    val timeIndexSetting = endIndexSetting - startIndexSetting

    val probeRDD_df = Adapter.toDf(probeRDD, sedona)
    val buildRDD_df = Adapter.toDf(buildRDD, sedona)

    // Repartition
    val probeRDD_repartition_df = probeRDD_df.repartition(1000)

    println(s"Binding name ${QueryInfo(3)}")
    probeRDD_df.createOrReplaceTempView(QueryInfo(3))
    println(s"Binding name ${QueryInfo(4)}")
    buildRDD_df.createOrReplaceTempView(QueryInfo(4))

    // Cold Run
    val startCold = System.currentTimeMillis()
    val resDFCold = sedona.sql(QueryInfo.last)
    val resCntCold = resDFCold.count()
    val endCold = System.currentTimeMillis()
    val timeCold= endCold - startCold

    // Hod Run
    val listTimeHotRun = new ListBuffer[Long]()
    val listResultHotRun = new ListBuffer[Long]()
    val sumTimeHotRun = 0.0
    for (n <- List.range(0, HotRunTimes)) {
      val startHot = System.currentTimeMillis()
      println(s"Run $n: Hot Start $startHot")

      // Query
      val resHot = sedona.sql(QueryInfo.last)
      listResultHotRun += resHot.count()

      val endHot = System.currentTimeMillis()
      println(s"Run $n: Hot End $endHot")
      val timeHot= endHot - startHot
      listTimeHotRun += timeHot

      //sumTimeHotRun = sumTimeHotRun + timeHot
    }
    val sumTime = listTimeHotRun.sum
    val avgHotTime = sumTime / HotRunTimes


    val strTag = "$$RESULT$$  "
    println(s"$strTag  User Defined Hot Run Times: $HotRunTimes\n")
    println(s"$strTag  IndexSetting Time: ${timeIndexSetting}")
    println(s"$strTag  Cold Run Time: ${timeCold}")

    println(s"")
    for (n <- List.range(0, HotRunTimes)) {
      println(s"$strTag  RUN $n Result: ${listResultHotRun(n)}")
      println(s"$strTag  RUN $n Time: ${listTimeHotRun(n)}")
    }
    println(s"")
    println(s"$strTag  Avg Hot Run Time: $avgHotTime")
  }

}

