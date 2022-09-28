/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import java.util.List;
import org.apache.commons.dbcp.BasicDataSource;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.inject.Singleton;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of {@link UserQuotaManager} that stores quota usage in mysql.
 */
@Slf4j
@Singleton
public class MysqlUserQuotaManager extends AbstractUserQuotaManager {
  public final MysqlQuotaStore quotaStore;
  public final RunningDagIdsStore runningDagIds;


  @Inject
  public MysqlUserQuotaManager(Config config) throws IOException {
    super(config);
    this.quotaStore = createQuotaStore(config);
    this.runningDagIds = createRunningDagStore(config);
  }

  void addDagId(Connection connection, String dagId) throws IOException {
    this.runningDagIds.add(connection, dagId);
  }

  @Override
  boolean containsDagId(String dagId) throws IOException {
    return this.runningDagIds.contains(dagId);
  }

  boolean removeDagId(Connection connection, String dagId) throws IOException {
    return this.runningDagIds.remove(connection, dagId);
  }

  // This implementation does not need to update quota usage when the service restarts or it's leadership status changes
  public void init(Collection<Dag<JobExecutionPlan>> dags) {
  }

  int incrementJobCount(Connection connection, String user, CountType countType) throws IOException, SQLException {
    return this.quotaStore.increaseCount(connection, user, countType);
  }

  void decrementJobCount(Connection connection,String user, CountType countType) throws IOException, SQLException {
      this.quotaStore.decreaseCount(user, countType);
  }

  @Override
  protected QuotaCheck increaseAndCheckQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    QuotaCheck quotaCheck = new QuotaCheck(true, true, true, "");
    Connection connection;
    try {
      connection = this.quotaStore.dataSource.getConnection();
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new IOException(e);
    }
    StringBuilder requesterMessage = new StringBuilder();

    // Dag is already being tracked, no need to double increment for retries and multihop flows
    try {
      if (containsDagId(DagManagerUtils.generateDagId(dagNode).toString())) {
        return quotaCheck;
      } else {
        addDagId(connection, DagManagerUtils.generateDagId(dagNode).toString());
      }

      String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
      String flowGroup = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(),
          ConfigurationKeys.FLOW_GROUP_KEY, "");
      String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

      boolean proxyUserCheck;

      if (proxyUser != null && dagNode.getValue().getCurrentAttempts() <= 1) {
        int proxyQuotaIncrement = incrementJobCountAndCheckQuota(connection,
            DagManagerUtils.getUserQuotaKey(proxyUser, dagNode), getQuotaForUser(proxyUser), CountType.USER_COUNT);
        proxyUserCheck = proxyQuotaIncrement >= 0;  // proxy user quota check succeeds
        quotaCheck.setProxyUserCheck(proxyUserCheck);
        if (!proxyUserCheck) {
          // add 1 to proxyUserIncrement since proxyQuotaIncrement is the count before the increment
          requesterMessage.append(String.format(
              "Quota exceeded for proxy user %s on executor %s : quota=%s, requests above quota=%d%n",
              proxyUser, specExecutorUri, getQuotaForUser(proxyUser), Math.abs(proxyQuotaIncrement) + 1 - getQuotaForUser(proxyUser)));
        }
      }

      String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
      boolean requesterCheck = true;

      if (dagNode.getValue().getCurrentAttempts() <= 1) {
        List<String> uniqueRequesters = DagManagerUtils.getDistinctUniqueRequesters(serializedRequesters);
        for (String requester : uniqueRequesters) {
          int userQuotaIncrement = incrementJobCountAndCheckQuota(connection, DagManagerUtils.getUserQuotaKey(requester, dagNode),
              getQuotaForUser(requester), CountType.REQUESTER_COUNT);
          boolean thisRequesterCheck = userQuotaIncrement >= 0;  // user quota check succeeds
          requesterCheck = requesterCheck && thisRequesterCheck;
          quotaCheck.setRequesterCheck(requesterCheck);
          if (!thisRequesterCheck) {
            requesterMessage.append(String.format("Quota exceeded for requester %s on executor %s : quota=%s, requests above quota=%d%n. ",
                requester, specExecutorUri, getQuotaForUser(requester), Math.abs(userQuotaIncrement) + 1 - getQuotaForUser(requester)));
          }
        }
      }

    boolean flowGroupCheck;

    if (dagNode.getValue().getCurrentAttempts() <= 1) {
      int flowGroupQuotaIncrement = incrementJobCountAndCheckQuota(connection,
          DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), getQuotaForFlowGroup(flowGroup), CountType.FLOWGROUP_COUNT);
      flowGroupCheck = flowGroupQuotaIncrement >= 0;
      quotaCheck.setFlowGroupCheck(flowGroupCheck);
      if (!flowGroupCheck) {
        requesterMessage.append(String.format("Quota exceeded for flowgroup %s on executor %s : quota=%s, requests above quota=%d%n",
            flowGroup, specExecutorUri, getQuotaForFlowGroup(flowGroup),
            Math.abs(flowGroupQuotaIncrement) + 1 - getQuotaForFlowGroup(flowGroup)));
      }
    }
      connection.commit();
    } catch (IOException | SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    } finally {
      try {
        connection.close();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    }

    quotaCheck.setRequesterMessage(requesterMessage.toString());

    return quotaCheck;
  }

  @Override
  protected void rollbackIncrements(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    String flowGroup = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), ConfigurationKeys.FLOW_GROUP_KEY, "");
    List<String> usersQuotaIncrement = DagManagerUtils.getDistinctUniqueRequesters(DagManagerUtils.getSerializedRequesterList(dagNode));
    Connection connection;
    try {
      connection = this.quotaStore.dataSource.getConnection();
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new IOException(e);
    }

    try {
      decrementJobCount(connection, DagManagerUtils.getUserQuotaKey(proxyUser, dagNode), CountType.USER_COUNT);
      decrementQuotaUsageForUsers(connection, usersQuotaIncrement);
      decrementJobCount(connection, DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), CountType.FLOWGROUP_COUNT);
      removeDagId(connection, DagManagerUtils.generateDagId(dagNode).toString());
    } catch (SQLException ex) {
      throw new IOException(ex);
    } finally {
      try {
        connection.close();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    }
  }

  protected int incrementJobCountAndCheckQuota(Connection connection, String key, int keyQuota, CountType countType)
      throws IOException, SQLException {
    int currentCount = incrementJobCount(connection, key, countType);
    if (currentCount >= keyQuota) {
      return -currentCount;
    } else {
      return currentCount;
    }
  }

  private void decrementQuotaUsageForUsers(Connection connection, List<String> requestersToDecreaseCount)
      throws IOException, SQLException {
    for (String requester : requestersToDecreaseCount) {
      decrementJobCount(connection, requester, CountType.REQUESTER_COUNT);
    }
  }

  /**
   * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link Dag.DagNode}.
   * Returns true if the dag existed in the set of running dags and was removed successfully
   */
  public boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    Connection connection;
    try {
      connection = this.quotaStore.dataSource.getConnection();
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new IOException(e);
    }

    try {
      boolean val = removeDagId(connection, DagManagerUtils.generateDagId(dagNode).toString());
      if (!val) {
        return false;
      }

      String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
      if (proxyUser != null) {
        String proxyUserKey = DagManagerUtils.getUserQuotaKey(proxyUser, dagNode);
        decrementJobCount(connection, proxyUserKey, CountType.USER_COUNT);
      }

      String flowGroup =
          ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), ConfigurationKeys.FLOW_GROUP_KEY, "");
      decrementJobCount(connection, DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), CountType.FLOWGROUP_COUNT);

      String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
      try {
        for (String requester : DagManagerUtils.getDistinctUniqueRequesters(serializedRequesters)) {
          String requesterKey = DagManagerUtils.getUserQuotaKey(requester, dagNode);
          decrementJobCount(connection, requesterKey, CountType.REQUESTER_COUNT);
        }
      } catch (IOException e) {
        log.error("Failed to release quota for requester list " + serializedRequesters, e);
        return false;
      }
    } catch (SQLException ex) {
      throw new IOException(ex);
    } finally {
      try {
        connection.close();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    }

    return true;
  }

  @VisibleForTesting
  int getCount(String name, CountType countType) throws IOException {
    return this.quotaStore.getCount(name, countType);
  }

  /**
   * Creating an instance of MysqlQuotaStore.
   */
  protected MysqlQuotaStore createQuotaStore(Config config) throws IOException {
    String quotaStoreTableName = ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_STORE_DB_TABLE_KEY,
        ServiceConfigKeys.DEFAULT_QUOTA_STORE_DB_TABLE);

    BasicDataSource basicDataSource = MysqlStateStore.newDataSource(config);

    return new MysqlQuotaStore(basicDataSource, quotaStoreTableName);
  }

  protected RunningDagIdsStore createRunningDagStore(Config config) throws IOException {
    String quotaStoreTableName = ConfigUtils.getString(config, ServiceConfigKeys.RUNNING_DAG_IDS_DB_TABLE_KEY,
        ServiceConfigKeys.DEFAULT_RUNNING_DAG_IDS_DB_TABLE);

    BasicDataSource basicDataSource = MysqlStateStore.newDataSource(config);

    return new RunningDagIdsStore(basicDataSource, quotaStoreTableName);
  }

  static class MysqlQuotaStore {
    protected final BasicDataSource dataSource;
    final String tableName;
    private final String GET_USER_COUNT;
    private final String GET_REQUESTER_COUNT;
    private final String GET_FLOWGROUP_COUNT;
    private final String INCREASE_USER_COUNT_SQL;
    private final String INCREASE_REQUESTER_COUNT_SQL;
    private final String INCREASE_FLOW_COUNT_SQL;
    private final String DECREASE_USER_COUNT_SQL;
    private final String DECREASE_REQUESTER_COUNT_SQL;
    private final String DECREASE_FLOWGROUP_COUNT_SQL;
    private final String DELETE_USER_SQL;

    public MysqlQuotaStore(BasicDataSource dataSource, String tableName)
        throws IOException {
      this.dataSource = dataSource;
      this.tableName = tableName;

      GET_USER_COUNT = "SELECT user_count FROM " + tableName + " WHERE name = ? FOR UPDATE";
      GET_REQUESTER_COUNT = "SELECT requester_count FROM " + tableName + " WHERE name = ? FOR UPDATE";
      GET_FLOWGROUP_COUNT = "SELECT flowgroup_count FROM " + tableName + " WHERE name = ? FOR UPDATE";
      INCREASE_USER_COUNT_SQL = "INSERT INTO " + tableName + " (name, user_count) VALUES (?, 1) "
          + "ON DUPLICATE KEY UPDATE user_count=user_count+1";
      INCREASE_REQUESTER_COUNT_SQL = "INSERT INTO " + tableName + " (name, requester_count) VALUES (?, 1) "
          + "ON DUPLICATE KEY UPDATE requester_count=requester_count+1";
      INCREASE_FLOW_COUNT_SQL = "INSERT INTO " + tableName + " (name, flowgroup_count) VALUES (?, 1) "
          + "ON DUPLICATE KEY UPDATE flowgroup_count=flowgroup_count+1";
      DECREASE_USER_COUNT_SQL = "UPDATE " + tableName + " SET user_count=GREATEST(0, user_count-1) WHERE name = ?";
      DECREASE_REQUESTER_COUNT_SQL = "UPDATE " + tableName + " SET requester_count=GREATEST(0, requester_count-1) WHERE name = ?";
      DECREASE_FLOWGROUP_COUNT_SQL = "UPDATE " + tableName + " SET flowgroup_count=flowgroup_count-1 WHERE name = ?";
      DELETE_USER_SQL = "DELETE FROM " + tableName + " WHERE name = ? AND user_count<1 AND flowgroup_count<1";

      String createQuotaTable = "CREATE TABLE IF NOT EXISTS " + tableName + " (name VARCHAR(20) CHARACTER SET latin1 NOT NULL, "
          + "user_count INT NOT NULL DEFAULT 0, requester_count INT NOT NULL DEFAULT 0, flowgroup_count INT NOT NULL DEFAULT 0, "
          + "PRIMARY KEY (name), " + "UNIQUE INDEX ind (name))";
      try (Connection connection = dataSource.getConnection(); PreparedStatement createStatement = connection.prepareStatement(createQuotaTable)) {
        createStatement.executeUpdate();
      } catch (SQLException e) {
        log.warn("Failure in creating table {}. Validation query is set to {} Exception is {}",
            tableName, this.dataSource.getValidationQuery(), e);
        throw new IOException(e);
      }
    }

    /**
     * returns count of countType for the name. if the row does not exist, returns zero.
     */
    @VisibleForTesting
    int getCount(String name, CountType countType) throws IOException {
      String selectStatement = countType == CountType.USER_COUNT ? GET_USER_COUNT : GET_FLOWGROUP_COUNT;
      try (Connection connection = dataSource.getConnection();
          PreparedStatement queryStatement = connection.prepareStatement(selectStatement)) {
        queryStatement.setString(1, name);
        try (ResultSet rs = queryStatement.executeQuery()) {
          if (rs.next()) {
            return rs.getInt(1);
          } else {
            return -1;
          }
        }
      } catch (Exception e) {
        throw new IOException("failure retrieving count from user/flowGroup " + name, e);
      }
    }

    public int increaseCount(Connection connection, String name, CountType countType) throws IOException, SQLException {
      String selectStatement;
      String increaseStatement;

      switch(countType) {
        case USER_COUNT:
          selectStatement = GET_USER_COUNT;
          increaseStatement = INCREASE_USER_COUNT_SQL;
          break;
        case REQUESTER_COUNT:
          selectStatement = GET_REQUESTER_COUNT;
          increaseStatement = INCREASE_REQUESTER_COUNT_SQL;
          break;
        case FLOWGROUP_COUNT:
          selectStatement = GET_FLOWGROUP_COUNT;
          increaseStatement = INCREASE_FLOW_COUNT_SQL;
          break;
        default:
          throw new IOException("Invalid count type " + countType);
      }

      ResultSet rs = null;
      try (PreparedStatement statement1 = connection.prepareStatement(selectStatement);
          PreparedStatement statement2 = connection.prepareStatement(increaseStatement)) {
        statement1.setString(1, name);
        statement2.setString(1, name);
        rs = statement1.executeQuery();
        statement2.executeUpdate();
        connection.commit();
        if (rs != null && rs.next()) {
          return rs.getInt(1);
        } else {
          return 0;
        }
      } finally {
        if (rs != null) {
          rs.close();
        }
      }
    }

    public void decreaseCount(String name, CountType countType) throws IOException, SQLException {
      Connection connection = dataSource.getConnection();
      connection.setAutoCommit(false);

      String selectStatement;
      String decreaseStatement;

      switch(countType) {
        case USER_COUNT:
          selectStatement = GET_USER_COUNT;
          decreaseStatement = DECREASE_USER_COUNT_SQL;
          break;
        case REQUESTER_COUNT:
          selectStatement = GET_REQUESTER_COUNT;
          decreaseStatement = DECREASE_REQUESTER_COUNT_SQL;
          break;
        case FLOWGROUP_COUNT:
          selectStatement = GET_FLOWGROUP_COUNT;
          decreaseStatement = DECREASE_FLOWGROUP_COUNT_SQL;
          break;
        default:
          throw new IOException("Invalid count type " + countType);
      }

      ResultSet rs = null;
      try (
          PreparedStatement statement1 = connection.prepareStatement(selectStatement);
          PreparedStatement statement2 = connection.prepareStatement(decreaseStatement);
          PreparedStatement statement3 = connection.prepareStatement(DELETE_USER_SQL)) {
        statement1.setString(1, name);
        statement2.setString(1, name);
        statement3.setString(1, name);
        rs = statement1.executeQuery();
        statement2.executeUpdate();
        statement3.executeUpdate();
        connection.commit();
        if (rs != null && rs.next() && rs.getInt(1) == 0) {
          log.warn("Decrement job count was called for " + name + " when the count was already zero/absent.");
        }
      } catch (SQLException e) {
        connection.rollback();
        throw new IOException("Failure decreasing count from user/flowGroup " + name, e);
      } finally {
        if (rs != null) {
          rs.close();
        }
        connection.close();
      }
    }
  }

  static class RunningDagIdsStore {
    protected final DataSource dataSource;
    final String tableName;
    private final String CONTAINS_DAG_ID;
    private final String ADD_DAG_ID;
    private final String REMOVE_DAG_ID;

    public RunningDagIdsStore(BasicDataSource dataSource, String tableName)
        throws IOException {
      this.dataSource = dataSource;
      this.tableName = tableName;

      CONTAINS_DAG_ID = "SELECT EXISTS(SELECT * FROM " + tableName + " WHERE dagId = ?)" ;
      ADD_DAG_ID = "INSERT INTO " + tableName + " (dagId) VALUES (?) ";
      REMOVE_DAG_ID = "DELETE FROM " + tableName + " WHERE dagId = ?";

      String createQuotaTable = "CREATE TABLE IF NOT EXISTS " + tableName + " (dagId VARCHAR(500) CHARACTER SET latin1 NOT NULL, "
          + "PRIMARY KEY (dagId), UNIQUE INDEX ind (dagId))";
      try (Connection connection = dataSource.getConnection(); PreparedStatement createStatement = connection.prepareStatement(createQuotaTable)) {
        createStatement.executeUpdate();
      } catch (SQLException e) {
        throw new IOException("Failure creation table " + tableName, e);
      }
    }

    /**
     * returns true if the DagID is already present in the running dag store
     */
    @VisibleForTesting
    boolean contains(String dagId) throws IOException {
      try (Connection connection = dataSource.getConnection();
          PreparedStatement queryStatement = connection.prepareStatement(CONTAINS_DAG_ID)) {
        queryStatement.setString(1, dagId);
        try (ResultSet rs = queryStatement.executeQuery()) {
          rs.next();
          return rs.getBoolean(1);
        }
      } catch (Exception e) {
        throw new IOException("Could not find if the dag " + dagId + " is already running.", e);
      }
    }

    public void add(Connection connection, String dagId) throws IOException {
      try (PreparedStatement statement = connection.prepareStatement(ADD_DAG_ID)) {
        statement.setString(1, dagId);
        statement.executeUpdate();
      } catch (SQLException e) {
        throw new IOException("Failure adding dag " + dagId, e);
      }
    }

    public boolean remove(Connection connection, String dagId) throws IOException {
      try (PreparedStatement statement = connection.prepareStatement(REMOVE_DAG_ID)) {
        statement.setString(1, dagId);
        int count = statement.executeUpdate();
        return count == 1;
      } catch (SQLException e) {
        throw new IOException("Could not remove dag " + dagId, e);
      }
    }
  }
}