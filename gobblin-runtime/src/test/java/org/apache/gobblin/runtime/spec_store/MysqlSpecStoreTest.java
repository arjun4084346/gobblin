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

package org.apache.gobblin.runtime.spec_store;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;


import org.apache.commons.lang3.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.FlowSpecSearchObject;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecSerDeException;
import org.apache.gobblin.runtime.spec_serde.GsonFlowSpecSerDe;
import org.apache.gobblin.service.FlowId;

import static org.apache.gobblin.service.ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY;
import static org.apache.gobblin.service.ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY;


public class MysqlSpecStoreTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "spec_store";

  private ITestMetastoreDatabase testDb;
  private MysqlSpecStore specStore;
  private MysqlSpecStore oldSpecStore;
  private final URI uri1 = FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowName("fg1").setFlowGroup("fn1"));
  private final URI uri2 = FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowName("fg2").setFlowGroup("fn2"));
  private final URI uri3 = FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowName("fg3").setFlowGroup("fn3"));
  private final URI uri4 = FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowName("fg4").setFlowGroup("fn4"));
  private final URI uri5 = FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowName("fg5").setFlowGroup("fn5"));

  private FlowSpec flowSpec1, flowSpec2, flowSpec3, flowSpec4,flowSpec5,flowSpec5Updated;

  public MysqlSpecStoreTest()
      throws URISyntaxException { // (based on `uri1` and other initializations just above)
  }

  @BeforeClass
  public void setUp() throws Exception {
    this.testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, this.testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.specStore = new MysqlSpecStore(config, new TestSpecSerDe());
    this.oldSpecStore = new OldSpecStore(config, new TestSpecSerDe());

    flowSpec1 = FlowSpec.builder(this.uri1)
        .withConfig(ConfigBuilder.create()
            .addPrimitive("key", "value")
            .addPrimitive("key3", "value3")
            .addPrimitive("filter.this.flow", true)
            .addPrimitive("config.with.dot", "value4")
            .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source")
            .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination")
            .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg1")
            .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn1").build())
        .withDescription("Test flow spec")
        .withVersion("Test version")
        .build();
    flowSpec2 = FlowSpec.builder(this.uri2)
        .withConfig(ConfigBuilder.create().addPrimitive("converter", "value1,value2,value3")
            .addPrimitive("key3", "value3")
            .addPrimitive("filter.this.flow", true)
            .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source")
            .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination")
            .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg2")
            .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn2").build())
        .withDescription("Test flow spec 2")
        .withVersion("Test version 2")
        .build();
    flowSpec3 = FlowSpec.builder(this.uri3)
        .withConfig(ConfigBuilder.create().addPrimitive("key3", "value3")
            .addPrimitive("filter.this.flow", true)
            .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source")
            .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination")
            .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg3")
            .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn3").build())
        .withDescription("Test flow spec 3")
        .withVersion("Test version 3")
        .build();

    flowSpec4 = FlowSpec.builder(this.uri4)
        .withConfig(ConfigBuilder.create().addPrimitive("key4", "value4")
            .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source")
            .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination")
            .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg4")
            .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn4")
            .addPrimitive(ConfigurationKeys.FLOW_OWNING_GROUP_KEY, "owningGroup4").build())
        .withDescription("Test flow spec 4")
        .withVersion("Test version 4")
        .build();
    flowSpec5=
        FlowSpec.builder(this.uri5)
            .withConfig(ConfigBuilder.create()
                .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source5")
                .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination5")
                .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, "0 0 8 * * ? *")
                .addPrimitive(ConfigurationKeys.FLOW_OWNING_GROUP_KEY, "owningGroup")
                .addPrimitive(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "true")
                .addPrimitive("user.to.proxy", "userA")
                .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg5")
                .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn5").build())
            .withDescription("Test flow spec 5")
            .withVersion("Test version 5")
            .build();

    flowSpec5Updated=
        FlowSpec.builder(this.uri5)
            .withConfig(ConfigBuilder.create()
                .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source5Updated")
                .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination5Updated")
                .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, "0 0 9 * * ? *")
                .addPrimitive(ConfigurationKeys.FLOW_OWNING_GROUP_KEY, "owningGroupUpdated")
                .addPrimitive(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "true")//not updated,kept same as flowSpec5
                .addPrimitive("user.to.proxy", "userAUpdated")
                .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg5")
                .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn5").build())
            .withDescription("Test flow spec 5")
            .withVersion("Test version 5")
            .build();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (this.testDb != null) {
      // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
      this.testDb.close();
    }
  }

  @Test(expectedExceptions = IOException.class)
  public void testSpecSearch() throws Exception {
    // empty FlowSpecSearchObject should throw an error
    FlowSpecSearchObject flowSpecSearchObject = FlowSpecSearchObject.builder().build();
    flowSpecSearchObject.augmentBaseGetStatement("SELECT * FROM Dummy WHERE ");
  }

  @Test
  public void testAddSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec1);
    this.specStore.addSpec(this.flowSpec2);
    this.specStore.addSpec(this.flowSpec4);

    Assert.assertEquals(this.specStore.getSize(), 3);
    Assert.assertTrue(this.specStore.exists(this.uri1));
    Assert.assertTrue(this.specStore.exists(this.uri2));
    Assert.assertTrue(this.specStore.exists(this.uri4));
    Assert.assertFalse(this.specStore.exists(URI.create("dummy")));
  }

  @Test(dependsOnMethods = "testAddSpec")
  public void testGetSpecs() throws Exception {
    FlowSpec result = (FlowSpec) this.specStore.getSpec(this.uri1);
    removeModificationTimestampFromSpecs(result);
    Assert.assertEquals(result, this.flowSpec1);

    Collection<Spec> specs = this.specStore.getSpecs();
    Assert.assertEquals(specs.size(), 3);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));

    Iterator<URI> uris = this.specStore.getSpecURIs();
    Assert.assertTrue(Iterators.contains(uris, this.uri1));
    Assert.assertTrue(Iterators.contains(uris, this.uri2));

    FlowSpecSearchObject flowSpecSearchObject = FlowSpecSearchObject.builder().flowGroup("fg1").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec1));

    flowSpecSearchObject = FlowSpecSearchObject.builder().flowName("fn2").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec2));

    flowSpecSearchObject = FlowSpecSearchObject.builder().flowName("fg1").flowGroup("fn2").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 0);

    flowSpecSearchObject = FlowSpecSearchObject.builder().propertyFilter("key=value").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec1));

    flowSpecSearchObject = FlowSpecSearchObject.builder().propertyFilter("converter=value2").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec2));

    flowSpecSearchObject = FlowSpecSearchObject.builder().propertyFilter("key3").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 2);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));

    flowSpecSearchObject = FlowSpecSearchObject.builder().propertyFilter("config.with.dot=value4").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec1));

    flowSpecSearchObject = FlowSpecSearchObject.builder().owningGroup("owningGroup4").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec4));
  }

  @Test
  public void testGetSpecsAfterUpdate() throws Exception {
    // Adding a flow spec to the spec store
    this.specStore.addSpec(this.flowSpec5);

    // Retrieving the flow spec from the store
    FlowSpec result = (FlowSpec) this.specStore.getSpec(this.uri5);

    // Modifying the retrieved result (removing modification timestamp) for equality check
    removeModificationTimestampFromSpecs(result);

    Assert.assertEquals(result, this.flowSpec5);

    List<Pair<FlowSpecSearchObject, Boolean>> preUpdateValidations = new ArrayList<>();
    List<Pair<FlowSpecSearchObject, Boolean>> postUpdateValidations = new ArrayList<>();

    populatePreAndPostUpdateFlowSpecSearchValidations(preUpdateValidations, postUpdateValidations);

    // Validating pre-update search results against flowSpec5
    for (Pair<FlowSpecSearchObject, Boolean> searchObjectBooleanPair : preUpdateValidations) {
      validateSearchResult(searchObjectBooleanPair, this.flowSpec5);
    }

    // Updating the flow spec in the spec store with flowSpec5Updated
    this.specStore.addSpec(this.flowSpec5Updated);

    // Validating post-update search results against flowSpec5Updated
    for (Pair<FlowSpecSearchObject, Boolean> searchObjectBooleanPair : postUpdateValidations) {
      validateSearchResult(searchObjectBooleanPair, this.flowSpec5Updated);
    }

    //Deleting the spec
    this.specStore.deleteSpec(this.flowSpec5Updated);

    FlowSpecSearchObject flowSpecSearchObject = FlowSpecSearchObject.builder().flowName("fn5").flowGroup("fg5").build();
    Collection<Spec> specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 0);
  }

  @Test(dependsOnMethods = "testGetSpecs")
  public void testGetSpecWithTag() throws Exception {

    //Creating and inserting flowspecs with tags
    URI uri5 = URI.create("flowspec5");
    FlowSpec flowSpec5 = FlowSpec.builder(uri5)
        .withConfig(ConfigBuilder.create()
            .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source")
            .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination")
            .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg5")
            .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn5")
            .addPrimitive("key5", "value5")
            .build())
        .withDescription("Test flow spec 5")
        .withVersion("Test version 5")
        .build();

    URI uri6 = URI.create("flowspec6");
    FlowSpec flowSpec6 = FlowSpec.builder(uri6)
        .withConfig(ConfigBuilder.create()
            .addPrimitive(FLOW_SOURCE_IDENTIFIER_KEY, "source")
            .addPrimitive(FLOW_DESTINATION_IDENTIFIER_KEY, "destination")
            .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "fg6")
            .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "fn6")
            .addPrimitive("key6", "value6")
            .build())
        .withDescription("Test flow spec 6")
        .withVersion("Test version 6")
        .build();

    this.specStore.addSpec(flowSpec5, "dr");
    this.specStore.addSpec(flowSpec6, "dr");

    Assert.assertTrue(this.specStore.exists(uri5));
    Assert.assertTrue(this.specStore.exists(uri6));
    List<URI> result = new ArrayList<>();
    this.specStore.getSpecURIsWithTag("dr").forEachRemaining(result::add);
    Assert.assertEquals(result.size(), 2);
  }

  @Test(dependsOnMethods = "testGetSpecs")
  public void testGetFilterSpecPaginate() throws Exception {
    /**
     * Sorted order of the specStore configurations is flowSpec1, flowSpec2.
     * flowSpec3 is not included as it is a 'corrupted' flowspec
     * flowSpec4 is not included as it doesn't have the 'filter.this.flow' property
     * Start is the offset of the first configuration to return
     * Count is the total number of configurations to return
     * PropertyFilter is the property to filter by
     */

    // Start of 0 and count of 1 means start from index 0, and return one configuration only
    FlowSpecSearchObject flowSpecSearchObject =
        FlowSpecSearchObject.builder().start(0).count(1).propertyFilter("filter.this.flow").build();
    Collection<Spec> specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertFalse(specs.contains(this.flowSpec2));
    Assert.assertFalse(specs.contains(this.flowSpec4));

    // Start of 1 and count of 1 means start from index 1, and return one configuration only
    flowSpecSearchObject = FlowSpecSearchObject.builder().start(1).count(1).propertyFilter("filter.this.flow").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 1);
    Assert.assertFalse(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));
    Assert.assertFalse(specs.contains(this.flowSpec4));

    /**
     * Start of 0 and count of 5 means start from index 0, and return five configuration only
     * Total of 3 flowSpecs in the DB, but flowSpec4 doesn't have 'filter.this.flow' filter so only returns 2 flowSpecs
     * flowSpec1 and flowSpec2 match all the criteria
     */
    flowSpecSearchObject = FlowSpecSearchObject.builder().start(0).count(5).propertyFilter("filter.this.flow").build();
    specs = this.specStore.getSpecs(flowSpecSearchObject);
    Assert.assertEquals(specs.size(), 2);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));
    Assert.assertFalse(specs.contains(this.flowSpec4));
  }

  /**
   * This method is used for testing to remove the `modification_timestamp` key from the FlowSpec config field for
   * only for testing purposes because the equality check between FlowSpec created for testing and retrieved from the
   * store will not be equal. In practice, we would never encounter this issue as we only compare specs obtained from
   * the store with the key mentioned.
   */
  public static void removeModificationTimestampFromSpecs(Spec spec) {
    ((FlowSpec) spec).getConfigAsProperties().remove(FlowSpec.MODIFICATION_TIME_KEY);
  }

  @Test(dependsOnMethods = "testGetSpecs")
  public void testGetAllSpecPaginate() throws Exception {
    /**
     * Sorted order of the specStore configurations is flowSpec1, flowSpec2, flowSpec4
     */
    // Return all flowSpecs from index 0 to 9. Total of 3 flowSpecs only so return all 3 flowSpecs
    Collection<Spec> specs = this.specStore.getSpecsPaginated(0, 10);
    specs.forEach(spec -> removeModificationTimestampFromSpecs(spec));
    Assert.assertEquals(specs.size(), 3);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));
    Assert.assertTrue(specs.contains(this.flowSpec4));

    // Return all flowSpecs using the default get all specs function. Testing default functionality of returning everything
    specs = this.specStore.getSpecs();
    specs.forEach(spec -> removeModificationTimestampFromSpecs(spec));
    Assert.assertEquals(specs.size(), 3);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));
    Assert.assertTrue(specs.contains(this.flowSpec4));

    // Return all flowSpecs from index 0 to 2 - 1. Total of 3 flowSpecs, only return first two.
    specs = this.specStore.getSpecsPaginated(0, 2);
    specs.forEach(spec -> removeModificationTimestampFromSpecs(spec));
    Assert.assertEquals(specs.size(), 2);
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));
    Assert.assertFalse(specs.contains(this.flowSpec4));

    // Return 0 flowSpecs when batch size is 0.
    specs = this.specStore.getSpecsPaginated(2, 0);
    Assert.assertEquals(specs.size(), 0);

    // Return 0 flowSpecs when start offset is past the end
    specs = this.specStore.getSpecsPaginated(3, 1);
    Assert.assertEquals(specs.size(), 0);

    // Check that we throw an error for incorrect inputs
    Assert.assertThrows(IllegalArgumentException.class, () -> this.specStore.getSpecsPaginated(-1, 2));
    Assert.assertThrows(IllegalArgumentException.class, () -> this.specStore.getSpecsPaginated(2, -4));
  }

  @Test(expectedExceptions = {IOException.class})
  public void testGetCorruptedSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec3);
  }

  @Test(dependsOnMethods = "testGetSpecWithTag")
  public void testDeleteSpec() throws Exception {
    Assert.assertEquals(this.specStore.getSize(), 5);
    this.specStore.deleteSpec(this.uri1);
    Assert.assertEquals(this.specStore.getSize(), 4);
    Assert.assertFalse(this.specStore.exists(this.uri1));
  }

  @Test(dependsOnMethods = "testDeleteSpec")
  public void testReadOldColumn() throws Exception {
    this.oldSpecStore.addSpec(this.flowSpec1);

    FlowSpec spec = (FlowSpec) this.specStore.getSpec(this.uri1);
    removeModificationTimestampFromSpecs(spec);
    Assert.assertEquals(spec, this.flowSpec1);
  }

  /**
   * A {@link MysqlSpecStore} which does not write into the new spec_json column
   * to simulate behavior of a table with old data.
   */
  public static class OldSpecStore extends MysqlSpecStore {

    public OldSpecStore(Config config, SpecSerDe specSerDe) throws IOException {
      super(config, specSerDe);
    }

    @Override
    public void addSpec(Spec spec, String tagValue) throws IOException {
      try (Connection connection = this.dataSource.getConnection();
          PreparedStatement statement = connection.prepareStatement(this.sqlStatements.insertStatement)) {
        this.sqlStatements.completeInsertPreparedStatement(statement, spec, tagValue);
        statement.setString(4, null);
        statement.executeUpdate();
        connection.commit();
      } catch (SQLException | SpecSerDeException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Validates the search result for a given FlowSpec against expected conditions.
   *
   * @param searchObjectBooleanPair A Pair containing the search object and a Boolean indicating the expected result.
   * @param flowSpec The FlowSpec to validate against the search results.
   * @throws IOException If there is an issue accessing data from the spec store.
   */
  private void validateSearchResult(Pair<FlowSpecSearchObject, Boolean> searchObjectBooleanPair,
      FlowSpec flowSpec) throws IOException {
    Collection<Spec> specs = this.specStore.getSpecs(searchObjectBooleanPair.first());
    Assert.assertEquals(Optional.of(specs.size() == 1).get(), searchObjectBooleanPair.second());
    Assert.assertEquals(Optional.of(specs.contains(flowSpec)).get(), searchObjectBooleanPair.second());
  }

  /**
   * Populates pre-update and post-update validations for FlowSpecs based on various search criteria.
   *
   * @param preUpdateValidations  List to store pairs of FlowSpecSearchObject and expected Boolean results
   *                              before updating.
   * @param postUpdateValidations List to store pairs of FlowSpecSearchObject and expected Boolean results
   *                              after updating.
   */
  private void populatePreAndPostUpdateFlowSpecSearchValidations(
      List<Pair<FlowSpecSearchObject, Boolean>> preUpdateValidations,
      List<Pair<FlowSpecSearchObject, Boolean>> postUpdateValidations) {

    FlowSpecSearchObject flowGroupFlowNameSearchOb =
        FlowSpecSearchObject.builder().flowName("fn5").flowGroup("fg5").build();
    preUpdateValidations.add(new Pair<>(flowGroupFlowNameSearchOb, true));
    postUpdateValidations.add(new Pair<>(flowGroupFlowNameSearchOb, true));

    FlowSpecSearchObject preUpdateSourceIdentifierSearchOb =
        FlowSpecSearchObject.builder().sourceIdentifier("source5").build();
    preUpdateValidations.add(new Pair<>(preUpdateSourceIdentifierSearchOb, true));
    postUpdateValidations.add(new Pair<>(preUpdateSourceIdentifierSearchOb, false));

    FlowSpecSearchObject postUpdateSourceIdentifierSearchOb =
        FlowSpecSearchObject.builder().sourceIdentifier("source5Updated").build();
    preUpdateValidations.add(new Pair<>(postUpdateSourceIdentifierSearchOb, false));
    postUpdateValidations.add(new Pair<>(postUpdateSourceIdentifierSearchOb, true));

    FlowSpecSearchObject preUpdateDestinationIdentifierSearchOb =
        FlowSpecSearchObject.builder().destinationIdentifier("destination5").build();
    preUpdateValidations.add(new Pair<>(preUpdateDestinationIdentifierSearchOb, true));
    postUpdateValidations.add(new Pair<>(preUpdateDestinationIdentifierSearchOb, false));
    FlowSpecSearchObject postUpdateDestinationIdentifierSearchOb =
        FlowSpecSearchObject.builder().destinationIdentifier("destination5Updated").build();
    preUpdateValidations.add(new Pair<>(postUpdateDestinationIdentifierSearchOb, false));
    postUpdateValidations.add(new Pair<>(postUpdateDestinationIdentifierSearchOb, true));

    //search with cronSchedule and runImmediately Filter
    FlowSpecSearchObject preUpdateJobScheduleSearchOb =
        FlowSpecSearchObject.builder().schedule("0 0 8 * * ? *").isRunImmediately(true).build();
    preUpdateValidations.add(new Pair<>(preUpdateJobScheduleSearchOb, true));
    postUpdateValidations.add(new Pair<>(preUpdateJobScheduleSearchOb, false));
    FlowSpecSearchObject postUpdateJobScheduleSearchOb =
        FlowSpecSearchObject.builder().schedule("0 0 9 * * ? *").isRunImmediately(true).build();
    preUpdateValidations.add(new Pair<>(postUpdateJobScheduleSearchOb, false));
    postUpdateValidations.add(new Pair<>(postUpdateJobScheduleSearchOb, true));

    //search with userToProxy and owningGroup Filter
    FlowSpecSearchObject preUpdateUserToProxySearchOb =
        FlowSpecSearchObject.builder().userToProxy("userA").owningGroup("owningGroup").build();
    preUpdateValidations.add(new Pair<>(preUpdateUserToProxySearchOb, true));
    postUpdateValidations.add(new Pair<>(preUpdateUserToProxySearchOb, false));
    FlowSpecSearchObject postUpdateUserToProxySearchOb =
        FlowSpecSearchObject.builder().userToProxy("userAUpdated").owningGroup("owningGroupUpdated").build();
    preUpdateValidations.add(new Pair<>(postUpdateUserToProxySearchOb, false));
    postUpdateValidations.add(new Pair<>(postUpdateUserToProxySearchOb, true));

    //Search with all the columns present in flowSpec5
    FlowSpecSearchObject preUpdateFullSearchValidator = FlowSpecSearchObject.builder()
        .flowGroup("fg5")
        .flowName("fn5")
        .sourceIdentifier("source5")
        .destinationIdentifier("destination5")
        .schedule("0 0 8 * * ? *")
        .userToProxy("userA")
        .owningGroup("owningGroup")
        .isRunImmediately(true)
        .build();
    preUpdateValidations.add(new Pair<>(preUpdateFullSearchValidator, true));
    postUpdateValidations.add(new Pair<>(preUpdateFullSearchValidator, false));

    //Search with all the columns present in flowSpec5Updated
    FlowSpecSearchObject postUpdateFullSearchValidator = FlowSpecSearchObject.builder()
        .flowGroup("fg5")
        .flowName("fn5")
        .sourceIdentifier("source5Updated")
        .destinationIdentifier("destination5Updated")
        .schedule("0 0 9 * * ? *")
        .userToProxy("userAUpdated")
        .owningGroup("owningGroupUpdated")
        .isRunImmediately(true)
        .build();

    postUpdateValidations.add(new Pair<>(postUpdateFullSearchValidator, true));
    preUpdateValidations.add(new Pair<>(postUpdateFullSearchValidator, false));
  }

  public class TestSpecSerDe extends GsonFlowSpecSerDe {
    @Override
    public byte[] serialize(Spec spec) throws SpecSerDeException {
      byte[] bytes = super.serialize(spec);
      // Reverse bytes to simulate corrupted Spec
      if (spec.getUri().equals(uri3)) {
        ArrayUtils.reverse(bytes);
      }
      return bytes;
    }
  }
}