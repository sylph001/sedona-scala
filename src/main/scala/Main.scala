/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import RddExample.{calculateSpatialColocation, visualizeSpatialColocation}
import TigerRddExample.{runTigerQuery, runQuerySQLAPI}
import SqlExample._
import VizExample._
import org.apache.log4j.{Level, Logger}
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator


object Main extends App {
  Logger.getRootLogger().setLevel(Level.ALL)

  val config = SedonaContext.builder().appName("SedonaSQL-demo")
    //.master("local[*]") // Please comment out this when use it on a cluster
    //.master("spark://9fcebbf32068:7077") // Please comment out this when use it on a cluster
    //.config("spark.driver.bindAddress", "0.0.0.0")
    //.config("sedona.global.index", "false")
    .config("sedona.global.index", "true") // test switch on
    //.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
    // -XZ
    //.config("sedona.join.autoBroadcastJoinThreshold","-1")
    //.config("spark.dynamicAllocation.enabled","true")
    //.config("spark.dynamicAllocation.shuffleTracking.enabled","true")
    //.config("spark.locality.wait.node","0")
    //.config("spark.scheduler.mode","FAIR")
    .getOrCreate()
    //.config("spark.driver.host", "127.0.0.1")
  val sedona = SedonaContext.create(config)

  SedonaVizRegistrator.registerAll(sedona)

  val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  /**
  for (n <- List.range(1,8)) {
    runTigerQuery(sedona, TigerRddExample.mapQueries(n), 3)
  }
  */

  val queryNum = args(0).toInt
  val apiNum = args(1).toInt
  val runTimes = args(2).toInt

  /*
  println(s"Query Num: ${queryNum}")
  println(s"Query to Run with RDD API: ${TigerRddExample.mapQueries(queryNum)}")
  runTigerQuery(sedona, TigerRddExample.mapQueries(queryNum), 1)
  System.out.println("All SedonaSQL DEMOs passed!")
   */

  val strTag = "$$RESULT$$  "
  println(s"${strTag}Query Number: ${queryNum}")
  println(s"${strTag}Using API: ${apiNum}")
  println(s"${strTag}Will Run Hot for ${runTimes} times")
  if (apiNum==1) { 
    val queryInfo = TigerRddExample.mapQueries(queryNum)
    println(s"${strTag}QUERY TO RUN WITH SQL API: ${queryInfo}")
    runTigerQuery(sedona,queryInfo,runTimes)
  } else if (apiNum==2) {
    val queryInfo = TigerRddExample.mapQuerySQL(queryNum)
    println(s"${strTag}QUERY TO RUN WITH SQL API: ${queryInfo}")
    runQuerySQLAPI(sedona, queryInfo, runTimes)
  } else {
    println(s"${strTag}Unknown API Name!")
  }
  println("${strTag}All SedonaSQL DEMOs done!")

}
