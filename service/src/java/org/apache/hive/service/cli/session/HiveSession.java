/**
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

package org.apache.hive.service.cli.session;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

public interface HiveSession extends HiveSessionBase {

  public void open();

  /**
   * getData reads a JSON string from a client
   * @param key
   * @return 
   */
  public String getData(String key);
  /**
   * setData sets data for a session given a key and value
   * @param key
   * @param value
   */
  public void setData(String key, String value);
  
  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException;

  /**
   * getInfo operation handler
   * @param getInfoType
   * @return
   * @throws HiveSQLException
   */
  public GetInfoValue getInfo(GetInfoType getInfoType) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle executeStatement(String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle executeStatementAsync(String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  /**
   * getTypeInfo operation handler
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getTypeInfo() throws HiveSQLException;

  /**
   * getCatalogs operation handler
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getCatalogs() throws HiveSQLException;

  /**
   * getSchemas operation handler
   * @param catalogName
   * @param schemaName
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException;

  /**
   * getTables operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param tableTypes
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getTables(String catalogName, String schemaName,
      String tableName, List<String> tableTypes) throws HiveSQLException;

  /**
   * getTableTypes operation handler
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getTableTypes() throws HiveSQLException ;

  /**
   * getColumns operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param columnName
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException;

  /**
   * getFunctions operation handler
   * @param catalogName
   * @param schemaName
   * @param functionName
   * @return
   * @throws HiveSQLException
   */
  public OperationHandle getFunctions(String catalogName, String schemaName,
      String functionName) throws HiveSQLException;

  /**
   * close the session
   * @throws HiveSQLException
   */
  public void close() throws HiveSQLException;

  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException;

  public void closeOperation(OperationHandle opHandle) throws HiveSQLException;

  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException;

  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException;

  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException;

  public String getDelegationToken(HiveAuthFactory authFactory, String owner,
      String renewer) throws HiveSQLException;

  public void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException;

  public void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException;
}
