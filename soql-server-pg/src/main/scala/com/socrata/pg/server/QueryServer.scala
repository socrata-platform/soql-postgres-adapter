package com.socrata.pg.server

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.rojoma.json.util.JsonUtil
import com.netflix.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceInstanceBuilder}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import com.socrata.datacoordinator.id.{ColumnId, DatasetId, RowId, UserColumnId}
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, CopyInfo, Schema}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.{SimpleRouteContext, SimpleResource}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.util.handlers.{LoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server._
import com.socrata.pg.query.{DataSqlizerQuerier, RowReaderQuerier}
import com.socrata.pg.server.config.QueryServerConfig
import com.socrata.pg.Schema._
import com.socrata.pg.SecondaryBase
import com.socrata.pg.soql.Sqlizer
import com.socrata.pg.store.{PGSecondaryUniverse, SchemaUtil, PGSecondaryRowReader}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.{SoQLVersion, SoQLValue, SoQLID, SoQLType}
import com.socrata.soql.typed.{ColumnRef, CoreExpr}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.log4j.PropertyConfigurator
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Executors, ExecutorService}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import java.sql.Connection
import java.io.ByteArrayInputStream


class QueryServer(val dsConfig: DataSourceConfig) extends SecondaryBase with Logging {

  private val routerSet = locally {
    import SimpleRouteContext._
    Routes(
      Route("/schema", SchemaResource),
      Route("/query", QueryResource),
      Route("/version", VersionResource)
    )
  }

  private def route(req: HttpServletRequest): HttpResponse = {
    routerSet(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound
    }
  }

  object VersionResource extends SimpleResource {
    val version = for {
      stream <- managed(getClass.getClassLoader.getResourceAsStream("soql-pg-version.json"))
      reader <- managed(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8))
    } yield com.rojoma.json.io.JsonReader.fromReader(reader)

    val response = OK ~> ContentType("application/json; charset=utf-8") ~> Content(version.toString)

    override val get = { req: HttpServletRequest => response }
  }

  object SchemaResource extends SimpleResource {
    override val get = schema _
  }

  def schema(req: HttpServletRequest): HttpResponse = {
    val ds = req.getParameter("ds")
    latestSchema(ds) match {
      case Some(schema) =>
        OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, schema, buffer = true))
      case None =>
        NotFound
    }
  }

  object QueryResource extends SimpleResource {
    override val get = query _
    override val post = query _
  }

  def query(req: HttpServletRequest): HttpServletResponse => Unit =  {

    val datasetId = req.getParameter("dataset")
    val analysisParam = req.getParameter("query")
    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(StandardCharsets.ISO_8859_1))
    val schemaHash = req.getParameter("schemaHash")
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.deserializer(analysisStream)
    val rowCount = Option(req.getParameter("rowCount")).map(_ == "approximate").getOrElse(false)
    logger.info("Performing query on dataset " + datasetId)

    withPgu() { pgu =>
      pgu.datasetInternalNameMapReader.datasetIdForInternalName(datasetId) match {
        case Some(dsId) =>
          execQuery(dsId, analysis)
        case None =>
          NotFound
      }
    }
  }

  def execQuery(datasetId: DatasetId, analysis: SoQLAnalysis[UserColumnId, SoQLType])(resp: HttpServletResponse) = {

    import Sqlizer._
    withPgu() { pgu =>
      pgu.datasetMapReader.datasetInfo(datasetId) match {
      case Some(datasetInfo) =>
        val latest: CopyInfo = pgu.datasetMapReader.latest(datasetInfo)
        val cryptProvider = new CryptProvider(datasetInfo.obfuscationKey)
        val idRep = new SoQLID.StringRep(cryptProvider)
        val verRep = new SoQLVersion.StringRep(cryptProvider)

        for (readCtx <- pgu.datasetReader.openDataset(latest)) {
          val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
          val systemToUserColumnMap =  SchemaUtil.systemToUserColumnMap(readCtx.schema)
          val userToSystemColumnMap = SchemaUtil.userToSystemColumnMap(readCtx.schema)
          val qrySchema = querySchema(pgu, analysis, latest)
          val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor(_))
          val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema)
          val sqlReps = querier.getSqlReps(systemToUserColumnMap)
          val results = querier.query(analysis, (a: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) =>
              (a, tableName).sql(sqlReps, Seq.empty, idRep, verRep),
              systemToUserColumnMap,
              userToSystemColumnMap,
              qryReps)
          logger.warn("TODO: Approximating the row count to 1000!")
          val approxRowCount = Option(1000L)

          val requestColumns =  qrySchema.values map {
            colInfo => colInfo.userColumnId
          }
          for ( r  <- results ) {
            CJSONWriter.writeCJson(datasetInfo, qrySchema, r, approxRowCount)(resp)
          }
        }
      case None =>
        NotFound(resp)
    }
    }
  }

  private def readerWithQuery[SoQLType, SoQLValue](conn: Connection,
                                                   pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                                   copyCtx: DatasetCopyContext[SoQLType],
                                                   schema: com.socrata.datacoordinator.util.collection.ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]]):
    PGSecondaryRowReader[SoQLType, SoQLValue] with RowReaderQuerier[SoQLType, SoQLValue] = {

    new PGSecondaryRowReader[SoQLType, SoQLValue] (
      conn,
      new PostgresRepBasedDataSqlizer(copyCtx.copyInfo.dataTableName, pgu.datasetContextFactory(schema), pgu.commonSupport.copyInProvider) with DataSqlizerQuerier[SoQLType, SoQLValue],
      pgu.commonSupport.timingReport) with RowReaderQuerier[SoQLType, SoQLValue]
  }

  /**
   * @param pgu
   * @param analysis parsed soql
   * @param latest
   * @return a schema for the selected columns
   */
  // TODO: Handle expressions and column aliases.
  private def querySchema(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                  analysis: SoQLAnalysis[UserColumnId, SoQLType],
                  latest: CopyInfo):
                  OrderedMap[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[pgu.CT]] = {

    analysis.selection.foldLeft(OrderedMap.empty[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[pgu.CT]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new com.socrata.datacoordinator.id.ColumnId(map.size + 1)
          val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[pgu.CT](
            latest,
            cid,
            new UserColumnId(columnName.name),
            coreExpr.typ,
            columnName.name,
            coreExpr.typ == SoQLID,
            false, // isUserKey
            coreExpr.typ == SoQLVersion
          )(SoQLTypeContext.typeNamespace, null)
          map + (cid -> cinfo)
      }
    }
  }

  /**
   * Get lastest schema
   * @param ds Data coordinator dataset id
   * @return Some schema or none
   */
  def latestSchema(ds: String): Option[Schema] = {
    withPgu() { pgu =>
      for {
        datasetId <- pgu.datasetInternalNameMapReader.datasetIdForInternalName(ds)
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val latest = pgu.datasetMapReader.latest(datasetInfo)
        pgu.datasetReader.openDataset(latest).map(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }
}

object QueryServer extends Logging {

  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.soql-server-pg.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryServerConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.soql-server-pg")
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  def main(args:Array[String]) {

    val address = config.advertisement.address
    val datasourceConfig = new DataSourceConfig(config.getRawConfig("store"), "database")

    implicit object executorResource extends com.rojoma.simplearm.Resource[ExecutorService] {
      def close(a: ExecutorService) { a.shutdown() }
    }

    for {
      curator <- managed(CuratorFrameworkFactory.builder.
        connectString(config.curator.ensemble).
        sessionTimeoutMs(config.curator.sessionTimeout.toMillis.toInt).
        connectionTimeoutMs(config.curator.connectTimeout.toMillis.toInt).
        retryPolicy(new retry.BoundedExponentialBackoffRetry(config.curator.baseRetryWait.toMillis.toInt,
        config.curator.maxRetryWait.toMillis.toInt,
        config.curator.maxRetries)).
        namespace(config.curator.namespace).
        build())
      discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
        client(curator).
        basePath(config.advertisement.basePath).
        build())
      pong <- managed(new LivenessCheckResponder(new InetSocketAddress(InetAddress.getByName(address), 0)))
      executor <- managed(Executors.newCachedThreadPool())
      dsInfo <- DataSourceFromConfig(datasourceConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
    } {
      curator.start()
      discovery.start()
      pong.start()
      val queryServer = new QueryServer(datasourceConfig)
      val auxData = new AuxiliaryData(livenessCheckInfo = Some(pong.livenessCheckInfo))
      val curatorBroker = new CuratorBroker(discovery, address, config.advertisement.name + "." + config.instance, Some(auxData))
      val handler = ThreadRenamingHandler(LoggingHandler(queryServer.route))
      val server = new SocrataServerJetty(handler, port = config.port, broker = curatorBroker)
      logger.info("starting pg query server")
      server.run()
    }
    logger.info("pg query server exited")
  }
}
