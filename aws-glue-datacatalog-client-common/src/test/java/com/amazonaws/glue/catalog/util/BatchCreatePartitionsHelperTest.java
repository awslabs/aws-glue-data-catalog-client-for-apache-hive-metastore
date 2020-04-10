package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.metastore.AWSGlueMetastore;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.InternalServiceException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.ResourceNumberLimitExceededException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.List;

import static com.amazonaws.glue.catalog.util.TestObjects.getPartitionError;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BatchCreatePartitionsHelperTest {

  @Mock
  private AWSGlueMetastore awsGlueMetastore;

  private BatchCreatePartitionsHelper batchCreatePartitionsHelper;

  private static final String NAMESPACE_NAME = "ns";
  private static final String TABLE_NAME = "table";

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreatePartitionsEmpty() throws Exception {
    mockBatchCreateSuccess();

    List<Partition> partitions = Lists.newArrayList();
    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, false)
        .createPartitions();

    assertTrue(batchCreatePartitionsHelper.getPartitionsCreated().isEmpty());
    assertNull(batchCreatePartitionsHelper.getFirstTException());
  }

  @Test
  public void testCreatePartitionsSucceed() throws Exception {
    mockBatchCreateSuccess();

    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    List<Partition> partitions = Lists.newArrayList(
        TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1),
        TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values2));
    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, false)
        .createPartitions();

    assertEquals(2, batchCreatePartitionsHelper.getPartitionsCreated().size());
    assertNull(batchCreatePartitionsHelper.getFirstTException());
    for (Partition partition : partitions) {
      assertTrue(batchCreatePartitionsHelper.getPartitionsCreated().contains(partition));
    }
    assertEquals(0, batchCreatePartitionsHelper.getPartitionsFailed().size());
  }

  @Test
  public void testCreatePartitionsThrowsException() throws Exception {
    Exception e = new RuntimeException("foo");
    mockBatchCreateThrowsException(e);

    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    List<Partition> partitions = Lists.newArrayList(
        TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1),
        TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values2));
    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, false);
    batchCreatePartitionsHelper.createPartitions();

    assertNotNull(batchCreatePartitionsHelper.getFirstTException());
    assertEquals("foo", batchCreatePartitionsHelper.getFirstTException().getMessage());
    assertEquals(partitions, batchCreatePartitionsHelper.getPartitionsFailed());
    assertTrue(batchCreatePartitionsHelper.getPartitionsCreated().isEmpty());
  }

  @Test
  public void testCreatePartitionsThrowsServiceExceptionAndPartitionPartiallyCreated() throws Exception {
    Exception e = new InternalServiceException("foo");
    mockBatchCreateThrowsException(e);
    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    List<String> values3 = Lists.newArrayList("val3");
    Partition partition1 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1);
    Partition partition2 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values2);
    Partition partition3 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values3);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2, partition3);
    Mockito.when(awsGlueMetastore.getPartition(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(partition1)
        .thenThrow(new EntityNotFoundException("bar"))
        .thenThrow(new NullPointerException("baz"));

    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, false)
        .createPartitions();

    assertThat(batchCreatePartitionsHelper.getFirstTException(), is(instanceOf(MetaException.class)));
    assertThat(batchCreatePartitionsHelper.getPartitionsCreated(), hasItems(partition1));
    assertThat(batchCreatePartitionsHelper.getPartitionsCreated(), not(hasItems(partition2, partition3)));
    assertThat(batchCreatePartitionsHelper.getPartitionsFailed(), hasItems(partition2, partition3));
    assertThat(batchCreatePartitionsHelper.getPartitionsFailed(), not(hasItems(partition1)));
  }

  @Test
  public void testCreatePartitionsDuplicateValues() throws Exception {
    mockBatchCreateSuccess();

    List<String> values1 = Lists.newArrayList("val1");
    Partition partition = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1);
    List<Partition> partitions = Lists.newArrayList(partition, partition);
    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, false)
        .createPartitions();

    assertEquals(1, batchCreatePartitionsHelper.getPartitionsCreated().size());
    assertNull(batchCreatePartitionsHelper.getFirstTException());
    for (Partition p : partitions) {
      assertTrue(batchCreatePartitionsHelper.getPartitionsCreated().contains(p));
    }
    assertTrue(batchCreatePartitionsHelper.getPartitionsFailed().isEmpty());
  }

  @Test
  public void testCreatePartitionsWithFailure() throws Exception {
    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    Partition partition1 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1);
    Partition partition2 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);

    PartitionError error = getPartitionError(values1, new AlreadyExistsException("foo error msg"));
    mockBatchCreateWithFailures(Lists.newArrayList(error));

    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, false)
        .createPartitions();

    assertEquals(1, batchCreatePartitionsHelper.getPartitionsCreated().size());
    assertThat(batchCreatePartitionsHelper.getPartitionsCreated(), hasItem(partition2));
    assertThat(batchCreatePartitionsHelper.getFirstTException(),
        is(instanceOf(org.apache.hadoop.hive.metastore.api.AlreadyExistsException.class)));
    assertThat(batchCreatePartitionsHelper.getPartitionsFailed(), hasItem(partition1));
  }

  @Test
  public void testCreatePartitionsWithFailureAllowExists() throws Exception {
    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    Partition partition1 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1);
    Partition partition2 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);

    PartitionError error = getPartitionError(values1, new AlreadyExistsException("foo error msg"));
    mockBatchCreateWithFailures(Lists.newArrayList(error));

    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, true)
        .createPartitions();

    assertEquals(1, batchCreatePartitionsHelper.getPartitionsCreated().size());
    assertThat(batchCreatePartitionsHelper.getPartitionsCreated(), hasItem(partition2));
    assertNull(batchCreatePartitionsHelper.getFirstTException());
    assertEquals(0, batchCreatePartitionsHelper.getPartitionsFailed().size());
  }

  @Test
  public void testCreatePartitionsWithFailures() throws Exception {
    List<String> values1 = Lists.newArrayList("val1");
    List<String> values2 = Lists.newArrayList("val2");
    Partition partition1 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values1);
    Partition partition2 = TestObjects.getTestPartition(NAMESPACE_NAME, TABLE_NAME, values2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);

    PartitionError error1 = getPartitionError(values1, new ResourceNumberLimitExceededException("foo error msg"));
    PartitionError error2 = getPartitionError(values2, new AlreadyExistsException("foo error msg2"));
    mockBatchCreateWithFailures(Lists.newArrayList(error1, error2));

    batchCreatePartitionsHelper = new BatchCreatePartitionsHelper(awsGlueMetastore, NAMESPACE_NAME, TABLE_NAME, null, partitions, true)
        .createPartitions();

    assertEquals(0, batchCreatePartitionsHelper.getPartitionsCreated().size());
    assertTrue(batchCreatePartitionsHelper.getFirstTException() instanceof MetaException);
    assertEquals(1, batchCreatePartitionsHelper.getPartitionsFailed().size());
    assertThat(batchCreatePartitionsHelper.getPartitionsFailed(), hasItem(partition1));
  }

  private void mockBatchCreateSuccess() {
    Mockito.when(awsGlueMetastore.createPartitions(Mockito.anyString(), Mockito.anyString(),
            Mockito.anyList())).thenReturn(null);
  }

  private void mockBatchCreateWithFailures(List<PartitionError> errors) {
    Mockito.when(awsGlueMetastore.createPartitions(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
        .thenReturn(errors);
  }

  private void mockBatchCreateThrowsException(Exception e) {
    Mockito.when(awsGlueMetastore.createPartitions(Mockito.anyString(), Mockito.anyString(),
            Mockito.anyList())).thenThrow(e);
  }

}