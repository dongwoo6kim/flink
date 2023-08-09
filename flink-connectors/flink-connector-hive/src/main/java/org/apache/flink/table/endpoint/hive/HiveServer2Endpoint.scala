/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.endpoint.hive

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.catalog.hive.client.HiveShimLoader
import org.apache.flink.table.factories.FactoryUtil
import org.apache.flink.table.gateway.api.SqlGatewayService
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint
import org.apache.flink.table.gateway.api.operation.OperationHandle
import org.apache.flink.table.gateway.api.operation.OperationStatus
import org.apache.flink.table.gateway.api.results.GatewayInfo
import org.apache.flink.table.gateway.api.results.OperationInfo
import org.apache.flink.table.gateway.api.results.ResultSet
import org.apache.flink.table.gateway.api.session.SessionEnvironment
import org.apache.flink.table.gateway.api.session.SessionHandle
import org.apache.flink.table.gateway.api.utils.SqlGatewayException
import org.apache.flink.table.gateway.api.utils.ThreadUtils
import org.apache.flink.util.ExceptionUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenReq
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenResp
import org.apache.hive.service.rpc.thrift.TCancelOperationReq
import org.apache.hive.service.rpc.thrift.TCancelOperationResp
import org.apache.hive.service.rpc.thrift.TCloseOperationReq
import org.apache.hive.service.rpc.thrift.TCloseOperationResp
import org.apache.hive.service.rpc.thrift.TCloseSessionReq
import org.apache.hive.service.rpc.thrift.TCloseSessionResp
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp
import org.apache.hive.service.rpc.thrift.TFetchResultsReq
import org.apache.hive.service.rpc.thrift.TFetchResultsResp
import org.apache.hive.service.rpc.thrift.TGetCatalogsReq
import org.apache.hive.service.rpc.thrift.TGetCatalogsResp
import org.apache.hive.service.rpc.thrift.TGetColumnsReq
import org.apache.hive.service.rpc.thrift.TGetColumnsResp
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceResp
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenReq
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenResp
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq
import org.apache.hive.service.rpc.thrift.TGetFunctionsResp
import org.apache.hive.service.rpc.thrift.TGetInfoReq
import org.apache.hive.service.rpc.thrift.TGetInfoResp
import org.apache.hive.service.rpc.thrift.TGetInfoValue
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysResp
import org.apache.hive.service.rpc.thrift.TGetQueryIdReq
import org.apache.hive.service.rpc.thrift.TGetQueryIdResp
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp
import org.apache.hive.service.rpc.thrift.TGetSchemasReq
import org.apache.hive.service.rpc.thrift.TGetSchemasResp
import org.apache.hive.service.rpc.thrift.TGetTableTypesReq
import org.apache.hive.service.rpc.thrift.TGetTableTypesResp
import org.apache.hive.service.rpc.thrift.TGetTablesReq
import org.apache.hive.service.rpc.thrift.TGetTablesResp
import org.apache.hive.service.rpc.thrift.TGetTypeInfoReq
import org.apache.hive.service.rpc.thrift.TGetTypeInfoResp
import org.apache.hive.service.rpc.thrift.TOpenSessionReq
import org.apache.hive.service.rpc.thrift.TOpenSessionResp
import org.apache.hive.service.rpc.thrift.TOperationHandle
import org.apache.hive.service.rpc.thrift.TOperationType
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenReq
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenResp
import org.apache.hive.service.rpc.thrift.TSetClientInfoReq
import org.apache.hive.service.rpc.thrift.TSetClientInfoResp
import org.apache.hive.service.rpc.thrift.TStatus
import org.apache.hive.service.rpc.thrift.TStatusCode
import org.apache.thrift.TException
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TTransportFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.annotation.Nullable
import java.net.InetSocketAddress
import java.time.Duration
import java.util.Collections
import java.util
import java.util.Objects
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE
import org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC
import org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT
import org.apache.flink.table.endpoint.hive.HiveServer2EndpointVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
import org.apache.flink.table.endpoint.hive.util.HiveJdbcParameterUtils.getUsedDefaultDatabase
import org.apache.flink.table.endpoint.hive.util.HiveJdbcParameterUtils.setVariables
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetCatalogsExecutor
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetColumnsExecutor
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetFunctionsExecutor
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetPrimaryKeys
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetSchemasExecutor
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetTableTypesExecutor
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetTablesExecutor
import org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetTypeInfoExecutor
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toFetchOrientation
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toFlinkTableKinds
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toOperationHandle
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toSessionHandle
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationHandle
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationState
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTRowSet
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTSessionHandle
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTStatus
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTTableSchema
import org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS
import org.apache.flink.util.Preconditions.checkNotNull

/**
 * HiveServer2 Endpoint that allows to accept the request from the hive client, e.g. Hive JDBC, Hive
 * Beeline.
 */
object HiveServer2Endpoint {
  private val LOG = LoggerFactory.getLogger(classOf[HiveServer2Endpoint])
  private val SERVER_VERSION = HIVE_CLI_SERVICE_PROTOCOL_V10
  private val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)
  private val UNSUPPORTED_ERROR_MESSAGE = "The HiveServer2 Endpoint currently doesn't support to %s."
  private val CHECK_INTERVAL_MS = 100L
}

class HiveServer2Endpoint @VisibleForTesting(

val service: SqlGatewayService, val socketAddress: InetSocketAddress, val maxMessageSize: Long, val requestTimeoutMs: Int, val backOffSlotLengthMs: Int, val minWorkerThreads: Int, val maxWorkerThreads: Int, val workerKeepAliveTime: Duration, val catalogName: String, val hiveConf: HiveConf, val defaultDatabase: String, val moduleName: String, val allowEmbedded: Boolean, val isVerbose: Boolean) extends TCLIService.Iface with SqlGatewayEndpoint with Runnable {
  this.workerKeepAliveTime = checkNotNull (workerKeepAliveTime)
  this.catalogName = checkNotNull (catalogName)
  final private var workerKeepAliveTime: Duration = null
  final private val serverThread: Thread = new Thread (this, "HiveServer2 Endpoint")
  private var executor: ThreadPoolExecutor = null
  private var server: TThreadPoolServer = null
  final private var catalogName: String = null
  def this (service: SqlGatewayService, socketAddress: InetSocketAddress, maxMessageSize: Long, requestTimeoutMs: Int, backOffSlotLengthMs: Int, minWorkerThreads: Int, maxWorkerThreads: Int, workerKeepAliveTime: Duration, catalogName: String, hiveConf: HiveConf, @Nullable defaultDatabase: String, moduleName: String) {
  this (service, socketAddress, maxMessageSize, requestTimeoutMs, backOffSlotLengthMs, minWorkerThreads, maxWorkerThreads, workerKeepAliveTime, catalogName, hiveConf, defaultDatabase, moduleName, false, true)
  }
  @throws[Exception]
  override def start (): Unit = {
  buildTThreadPoolServer ()
  serverThread.start ()
  }
  @throws[Exception]
  override def stop (): Unit = {
  if (server != null) {
  server.stop ()
  }
  if (executor != null) {
  executor.shutdownNow
  }
  }
  @throws[TException]
  override def OpenSession (tOpenSessionReq: TOpenSessionReq): TOpenSessionResp = {
  HiveServer2Endpoint.LOG.debug ("Client protocol version: {}.", tOpenSessionReq.getClient_protocol)
  val resp: TOpenSessionResp = new TOpenSessionResp
  try { // negotiate connection protocol
  val clientProtocol: TProtocolVersion = tOpenSessionReq.getClient_protocol
  // the session version is not larger than the server version because of the
  // min(server_version, ...)
  val sessionVersion: HiveServer2EndpointVersion = HiveServer2EndpointVersion.valueOf (TProtocolVersion.findByValue (Math.min (clientProtocol.getValue, HiveServer2Endpoint.SERVER_VERSION.getVersion.getValue) ) )
  // prepare session environment
  val originSessionConf: util.Map[String, String] = if (tOpenSessionReq.getConfiguration == null) {
  Collections.emptyMap
  }
  else {
  tOpenSessionReq.getConfiguration
  }
  val conf: HiveConf = new HiveConf (hiveConf)
  val hiveCatalog: Catalog = new HiveCatalog (catalogName, getUsedDefaultDatabase (originSessionConf).orElse (defaultDatabase), conf, HiveShimLoader.getHiveVersion, allowEmbedded)
  // Trigger the creation of the HiveMetaStoreClient to use the same HiveConf. If the
  // initial HiveConf is different, it will trigger the PersistenceManagerFactory to close
  // all the alive PersistenceManager in the ObjectStore, which may get error like
  // "Persistence Manager has been closed" in the later connection.
  hiveCatalog.open ()
  // create hive module lazily
  val hiveModuleCreator: SessionEnvironment.ModuleCreator = (readableConfig: ReadableConfig, classLoader: ClassLoader) => FactoryUtil.createModule (moduleName, Collections.emptyMap, readableConfig, classLoader)
  // set variables to HiveConf and Session's conf
  val sessionConfig: util.Map[String, String] = new util.HashMap[String, String]
  sessionConfig.put (TABLE_SQL_DIALECT.key, SqlDialect.HIVE.name)
  sessionConfig.put (RUNTIME_MODE.key, RuntimeExecutionMode.BATCH.name)
  sessionConfig.put (TABLE_DML_SYNC.key, "true")
  setVariables (conf, sessionConfig, originSessionConf)
  val sessionHandle: SessionHandle = service.openSession (SessionEnvironment.newBuilder.setSessionEndpointVersion (sessionVersion).registerCatalogCreator (catalogName, (readableConfig: ReadableConfig, classLoader: ClassLoader) => hiveCatalog).registerModuleCreatorAtHead (moduleName, hiveModuleCreator).setDefaultCatalog (catalogName).addSessionConfig (sessionConfig).build)
  // response
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setServerProtocolVersion (sessionVersion.getVersion)
  resp.setSessionHandle (toTSessionHandle (sessionHandle) )
  resp.setConfiguration (service.getSessionConfig (sessionHandle) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to OpenSession.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def CloseSession (tCloseSessionReq: TCloseSessionReq): TCloseSessionResp = {
  val resp: TCloseSessionResp = new TCloseSessionResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tCloseSessionReq.getSessionHandle)
  service.closeSession (sessionHandle)
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to CloseSession.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetInfo (tGetInfoReq: TGetInfoReq): TGetInfoResp = {
  val resp: TGetInfoResp = new TGetInfoResp
  try {
  val info: GatewayInfo = service.getGatewayInfo
  var tInfoValue: TGetInfoValue = null
  tGetInfoReq.getInfoType match {
  case CLI_SERVER_NAME =>
  case CLI_DBMS_NAME =>
  tInfoValue = TGetInfoValue.stringValue (info.getProductName)

  case CLI_DBMS_VER =>
  tInfoValue = TGetInfoValue.stringValue (info.getVersion.toString)

  case _ =>
  throw new UnsupportedOperationException (String.format ("Unrecognized TGetInfoType value: %s.", tGetInfoReq.getInfoType) )
  }
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setInfoValue (tInfoValue)
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetInfo.", t)
  // InfoValue must be set because the hive service requires it.
  resp.setInfoValue (TGetInfoValue.lenValue (0) )
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def ExecuteStatement (tExecuteStatementReq: TExecuteStatementReq): TExecuteStatementResp = {
  val resp: TExecuteStatementResp = new TExecuteStatementResp
  val sessionHandle: SessionHandle = toSessionHandle (tExecuteStatementReq.getSessionHandle)
  var operationHandle: OperationHandle = null
  try {
  val statement: String = if (tExecuteStatementReq.isSetStatement) {
  tExecuteStatementReq.getStatement
  }
  else {
  ""
  }
  val executionConfig: util.Map[String, String] = if (tExecuteStatementReq.isSetConfOverlay) {
  tExecuteStatementReq.getConfOverlay
  }
  else {
  Collections.emptyMap
  }
  val timeout: Long = tExecuteStatementReq.getQueryTimeout
  operationHandle = service.executeStatement (sessionHandle, statement, timeout, Configuration.fromMap (executionConfig) )
  if (! (tExecuteStatementReq.isRunAsync) ) {
  waitUntilOperationIsTerminated (sessionHandle, operationHandle)
  }
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.EXECUTE_STATEMENT) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to ExecuteStatement.", t)
  resp.setStatus (toTStatus (t) )
  if (operationHandle != null) {
  closeOperationSilently (sessionHandle, operationHandle)
  }
  }
  return resp
  }
  @throws[TException]
  override def GetTypeInfo (tGetTypeInfoReq: TGetTypeInfoReq): TGetTypeInfoResp = {
  val resp: TGetTypeInfoResp = new TGetTypeInfoResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetTypeInfoReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetTypeInfoExecutor)
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_TYPE_INFO) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetTypeInfo.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetCatalogs (tGetCatalogsReq: TGetCatalogsReq): TGetCatalogsResp = {
  val resp: TGetCatalogsResp = new TGetCatalogsResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetCatalogsReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetCatalogsExecutor (service, sessionHandle) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_CATALOGS) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetCatalogs.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetSchemas (tGetSchemasReq: TGetSchemasReq): TGetSchemasResp = {
  val resp: TGetSchemasResp = new TGetSchemasResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetSchemasReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetSchemasExecutor (service, sessionHandle, tGetSchemasReq.getCatalogName, tGetSchemasReq.getSchemaName) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_SCHEMAS) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetSchemas.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetTables (tGetTablesReq: TGetTablesReq): TGetTablesResp = {
  val resp: TGetTablesResp = new TGetTablesResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetTablesReq.getSessionHandle)
  val tableKinds: util.Set[CatalogBaseTable.TableKind] = toFlinkTableKinds (tGetTablesReq.getTableTypes)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetTablesExecutor (service, sessionHandle, tGetTablesReq.getCatalogName, tGetTablesReq.getSchemaName, tGetTablesReq.getTableName, tableKinds) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_TABLES) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetTables.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetTableTypes (tGetTableTypesReq: TGetTableTypesReq): TGetTableTypesResp = {
  val resp: TGetTableTypesResp = new TGetTableTypesResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetTableTypesReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetTableTypesExecutor)
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_TABLES) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetTableTypes.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetColumns (tGetColumnsReq: TGetColumnsReq): TGetColumnsResp = {
  val resp: TGetColumnsResp = new TGetColumnsResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetColumnsReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetColumnsExecutor (service, sessionHandle, tGetColumnsReq.getCatalogName, tGetColumnsReq.getSchemaName, tGetColumnsReq.getTableName, tGetColumnsReq.getColumnName) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_COLUMNS) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetColumns.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetFunctions (tGetFunctionsReq: TGetFunctionsReq): TGetFunctionsResp = {
  val resp: TGetFunctionsResp = new TGetFunctionsResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetFunctionsReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetFunctionsExecutor (service, sessionHandle, tGetFunctionsReq.getCatalogName, tGetFunctionsReq.getSchemaName, tGetFunctionsReq.getFunctionName) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle (sessionHandle, operationHandle, TOperationType.GET_FUNCTIONS) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetFunctions.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetPrimaryKeys (tGetPrimaryKeysReq: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
  val resp: TGetPrimaryKeysResp = new TGetPrimaryKeysResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetPrimaryKeysReq.getSessionHandle)
  val operationHandle: OperationHandle = service.submitOperation (sessionHandle, createGetPrimaryKeys (service, sessionHandle, tGetPrimaryKeysReq.getCatalogName, tGetPrimaryKeysReq.getSchemaName, tGetPrimaryKeysReq.getTableName) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setOperationHandle (toTOperationHandle ( // hive's implementation use "GET_FUNCTIONS" here
  sessionHandle, operationHandle, TOperationType.GET_FUNCTIONS) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetPrimaryKeys.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetCrossReference (tGetCrossReferenceReq: TGetCrossReferenceReq): TGetCrossReferenceResp = {
  return new TGetCrossReferenceResp (buildErrorStatus ("GetCrossReference") )
  }
  @throws[TException]
  override def GetOperationStatus (tGetOperationStatusReq: TGetOperationStatusReq): TGetOperationStatusResp = {
  val resp: TGetOperationStatusResp = new TGetOperationStatusResp
  try {
  val operationHandle: TOperationHandle = tGetOperationStatusReq.getOperationHandle
  val operationInfo: OperationInfo = service.getOperationInfo (toSessionHandle (operationHandle), toOperationHandle (operationHandle) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  // TODO: support completed time / start time
  resp.setOperationState (toTOperationState (operationInfo.getStatus) )
  // Currently, all operations have results.
  resp.setHasResultSet (true)
  if (operationInfo.getStatus == OperationStatus.ERROR && operationInfo.getException.isPresent) {
  resp.setErrorMessage (stringifyException (operationInfo.getException.get) )
  }
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to GetOperationStatus.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def CancelOperation (tCancelOperationReq: TCancelOperationReq): TCancelOperationResp = {
  val resp: TCancelOperationResp = new TCancelOperationResp
  try {
  val operationHandle: TOperationHandle = tCancelOperationReq.getOperationHandle
  service.cancelOperation (toSessionHandle (operationHandle), toOperationHandle (operationHandle) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to CancelOperation.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def CloseOperation (tCloseOperationReq: TCloseOperationReq): TCloseOperationResp = {
  val resp: TCloseOperationResp = new TCloseOperationResp
  try {
  val operationHandle: TOperationHandle = tCloseOperationReq.getOperationHandle
  service.closeOperation (toSessionHandle (operationHandle), toOperationHandle (operationHandle) )
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to CloseOperation.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetResultSetMetadata (tGetResultSetMetadataReq: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
  val resp: TGetResultSetMetadataResp = new TGetResultSetMetadataResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tGetResultSetMetadataReq.getOperationHandle)
  val operationHandle: OperationHandle = toOperationHandle (tGetResultSetMetadataReq.getOperationHandle)
  val schema: ResolvedSchema = service.getOperationResultSchema (sessionHandle, operationHandle)
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setSchema (toTTableSchema (schema) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.warn ("Failed to GetResultSetMetadata.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def FetchResults (tFetchResultsReq: TFetchResultsReq): TFetchResultsResp = {
  if (tFetchResultsReq.getFetchType != 0) { // Don't log the annoying messages because Hive beeline will fetch the logs until
  // operation is terminated.
  return new TFetchResultsResp (toTStatus (new UnsupportedOperationException ("The HiveServer2 endpoint currently doesn't support to fetch logs.") ) )
  }
  val resp: TFetchResultsResp = new TFetchResultsResp
  try {
  val sessionHandle: SessionHandle = toSessionHandle (tFetchResultsReq.getOperationHandle)
  val operationHandle: OperationHandle = toOperationHandle (tFetchResultsReq.getOperationHandle)
  if (tFetchResultsReq.getMaxRows > Integer.MAX_VALUE) {
  throw new SqlGatewayException (String.format ("The SqlGateway doesn't support to fetch more that %s rows.", Integer.MAX_VALUE) )
  }
  if (tFetchResultsReq.getMaxRows < 0) {
  throw new IllegalArgumentException (String.format ("SqlGateway doesn't support to fetch %s rows. Please specify a positive value for the max rows.", tFetchResultsReq.getMaxRows) )
  }
  val maxRows: Int = tFetchResultsReq.getMaxRows.toInt
  val resultSet: ResultSet = service.fetchResults (sessionHandle, operationHandle, toFetchOrientation (tFetchResultsReq.getOrientation), maxRows)
  resp.setStatus (HiveServer2Endpoint.OK_STATUS)
  resp.setHasMoreRows (resultSet.getResultType ne EOS)
  val sessionEndpointVersion: EndpointVersion = service.getSessionEndpointVersion (sessionHandle)
  if (! ((sessionEndpointVersion.isInstanceOf[HiveServer2EndpointVersion] ) ) ) {
  throw new SqlGatewayException (String.format ("The specified endpoint version %s is not %s.", sessionEndpointVersion.getClass.getCanonicalName, classOf[HiveServer2EndpointVersion].getCanonicalName) )
  }
  resp.setResults (toTRowSet ((sessionEndpointVersion.asInstanceOf[HiveServer2EndpointVersion] ).getVersion, resultSet.getResultSchema, resultSet.getData) )
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Failed to FetchResults.", t)
  resp.setStatus (toTStatus (t) )
  }
  return resp
  }
  @throws[TException]
  override def GetDelegationToken (tGetDelegationTokenReq: TGetDelegationTokenReq): TGetDelegationTokenResp = {
  return new TGetDelegationTokenResp (buildErrorStatus ("GetDelegationToken") )
  }
  @throws[TException]
  override def CancelDelegationToken (tCancelDelegationTokenReq: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
  return new TCancelDelegationTokenResp (buildErrorStatus ("CancelDelegationToken") )
  }
  @throws[TException]
  override def RenewDelegationToken (tRenewDelegationTokenReq: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
  return new TRenewDelegationTokenResp (buildErrorStatus ("RenewDelegationToken") )
  }

  /** To be compatible with Hive3, add a default implementation. */
  @throws[TException]
  def GetQueryId (tGetQueryIdReq: TGetQueryIdReq): TGetQueryIdResp = {
  throw new TException (new UnsupportedOperationException (String.format (HiveServer2Endpoint.UNSUPPORTED_ERROR_MESSAGE, "GetQueryId") ) )
  }
  @throws[TException]
  def SetClientInfo (tSetClientInfoReq: TSetClientInfoReq): TSetClientInfoResp = {
  return new TSetClientInfoResp (buildErrorStatus ("SetClientInfo") )
  }
  override def equals (o: Any): Boolean = {
  if (this eq o) {
  return true
  }
  if (! ((o.isInstanceOf[HiveServer2Endpoint] ) ) ) {
  return false
  }
  val that: HiveServer2Endpoint = o.asInstanceOf[HiveServer2Endpoint]
  return Objects.equals (socketAddress, that.socketAddress) && minWorkerThreads == that.minWorkerThreads && maxWorkerThreads == that.maxWorkerThreads && requestTimeoutMs == that.requestTimeoutMs && backOffSlotLengthMs == that.backOffSlotLengthMs && maxMessageSize == that.maxMessageSize && Objects.equals (workerKeepAliveTime, that.workerKeepAliveTime) && Objects.equals (catalogName, that.catalogName) && Objects.equals (defaultDatabase, that.defaultDatabase) && Objects.equals (allowEmbedded, that.allowEmbedded) && Objects.equals (isVerbose, that.isVerbose) && Objects.equals (moduleName, that.moduleName)
  }
  override def hashCode: Int = {
  return Objects.hash (socketAddress, minWorkerThreads, maxWorkerThreads, workerKeepAliveTime, requestTimeoutMs, backOffSlotLengthMs, maxMessageSize, catalogName, defaultDatabase, allowEmbedded, isVerbose, moduleName)
  }
  override def run (): Unit = {
  try {
  HiveServer2Endpoint.LOG.info ("HiveServer2 Endpoint begins to listen on {}.", socketAddress.toString)
  server.serve ()
  } catch {
  case t: Throwable =>
  HiveServer2Endpoint.LOG.error ("Exception caught by " + getClass.getSimpleName + ". Exiting.", t)
  System.exit (- (1) )
  }
  }
  private def buildTThreadPoolServer (): Unit = {
  executor = ThreadUtils.newThreadPool (minWorkerThreads, maxWorkerThreads, workerKeepAliveTime.toMillis, "hiveserver2-endpoint-thread-pool")
  try server = new TThreadPoolServer (new TThreadPoolServer.Args (new TServerSocket (socketAddress) ).processorFactory (new TProcessorFactory (new TCLIService.Processor[HiveServer2Endpoint] (this) ) ).transportFactory (new TTransportFactory).protocolFactory // Currently, only support binary mode.
  (new TBinaryProtocol.Factory).inputProtocolFactory (new TBinaryProtocol.Factory (true, true, maxMessageSize, maxMessageSize) ).requestTimeout (requestTimeoutMs).requestTimeoutUnit (TimeUnit.MILLISECONDS).beBackoffSlotLength (backOffSlotLengthMs).beBackoffSlotLengthUnit (TimeUnit.MILLISECONDS).executorService (executor) )
  catch {
  case e: Exception =>
  throw new SqlGatewayException ("Failed to build the server.", e)
  }
  }

  /**
   * Similar solution comparing to the {@code
   * org.apache.hive.jdbc.HiveStatement#waitForOperationToComplete}.
   *
   * <p>The better solution is to introduce an interface similar to {@link TableResult # await ( )}.
   */
  @throws[Exception]
  private def waitUntilOperationIsTerminated (sessionHandle: SessionHandle, operationHandle: OperationHandle): Unit = {
  var info: OperationInfo = null
  do { {
  info = service.getOperationInfo (sessionHandle, operationHandle)
  info.getStatus match {
  case INITIALIZED =>
  case PENDING =>
  case RUNNING =>
  Thread.sleep (HiveServer2Endpoint.CHECK_INTERVAL_MS)

  case CANCELED =>
  case TIMEOUT =>
  throw new SqlGatewayException (String.format ("The operation %s's status is %s for the session %s.", operationHandle, info.getStatus, sessionHandle) )
  case ERROR =>
  throw new SqlGatewayException (String.format ("The operation %s's status is %s for the session %s.", operationHandle, info.getStatus, sessionHandle), info.getException.orElseThrow (() => new SqlGatewayException ("Impossible! ERROR status should contains the error.") ) )
  case FINISHED =>
  return
  case _ =>
  throw new SqlGatewayException (String.format ("Unknown status: %s.", info.getStatus) )
  }
  }
  } while ( {
  true
  })
  }
  private def closeOperationSilently (sessionHandle: SessionHandle, operationHandle: OperationHandle): Unit = {
  try service.closeOperation (sessionHandle, operationHandle)
  catch {
  case t: Throwable =>
  // ignore
  HiveServer2Endpoint.LOG.error (String.format ("Close the operation %s for the session %s silently.", operationHandle, sessionHandle), t)
  }
  }
  private def stringifyException (t: Throwable): String = {
  if (isVerbose) {
  return ExceptionUtils.stringifyException (t)
  }
  else {
  var root: Throwable = t
  while ( {
  root.getCause != null && root.getCause.getMessage != null && ! (root.getCause.getMessage.isEmpty)
  }) {
  root = root.getCause
  }
  return root.getClass.getName + ": " + root.getMessage
  }
  }
  private def buildErrorStatus (methodName: String): TStatus = {
  return toTStatus (new UnsupportedOperationException (String.format (HiveServer2Endpoint.UNSUPPORTED_ERROR_MESSAGE, methodName) ) )
  }
  }
