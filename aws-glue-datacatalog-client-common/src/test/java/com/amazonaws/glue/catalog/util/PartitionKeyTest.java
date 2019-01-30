package com.amazonaws.glue.catalog.util;

import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PartitionKeyTest {

  @Test(expected=IllegalArgumentException.class)
  public void testNull() {
    new PartitionKey((List<String>) null);
  }

  @Test
  public void testEqualsDifferentTable() {
    List<String> values1 = Lists.newArrayList("value1", "value2");
    List<String> values2 = Lists.newArrayList("value1", "value2");
    Partition partition1 = TestObjects.getTestPartition("ns", "table1", values1);
    Partition partition2 = TestObjects.getTestPartition("ns", "table2", values2);
    PartitionKey partitionKey1 = new PartitionKey(partition1);
    PartitionKey partitionKey2 = new PartitionKey(partition2);
    assertEquals(partitionKey1, partitionKey2);
    assertEquals(partitionKey1.hashCode(), partitionKey2.hashCode());
  }

  @Test
  public void testEqualsEmptyValue() {
    List<String> values = Lists.newArrayList();
    Partition partition1 = TestObjects.getTestPartition("ns", "table", values);
    Partition partition2 = TestObjects.getTestPartition("ns", "table", values);
    PartitionKey partitionKey1 = new PartitionKey(partition1);
    PartitionKey partitionKey2 = new PartitionKey(partition2);
    assertEquals(partitionKey1, partitionKey2);
    assertEquals(partitionKey1.hashCode(), partitionKey2.hashCode());
  }

  @Test
  public void testEqualsDifferentClass() {
    List<String> values1 = Lists.newArrayList("value1", "value2");
    List<String> values2 = Lists.newLinkedList(values1);
    Partition partition1 = TestObjects.getTestPartition("ns", "table", values1);
    Partition partition2 = TestObjects.getTestPartition("ns", "table", values2);
    PartitionKey partitionKey1 = new PartitionKey(partition1);
    PartitionKey partitionKey2 = new PartitionKey(partition2);
    assertEquals(partitionKey1, partitionKey2);
    assertEquals(partitionKey1.hashCode(), partitionKey2.hashCode());
  }

  @Test
  public void testEqualsPartitionError() {
    List<String> values1 = Lists.newArrayList("value1", "value2");
    List<String> values2 = Lists.newArrayList("value1", "value2");
    PartitionError partitionError1 = TestObjects.getPartitionError(values1, new RuntimeException("foo"));
    PartitionError partitionError2 = TestObjects.getPartitionError(values2, new Exception("foo2"));
    PartitionKey partitionKey1 = new PartitionKey(partitionError1.getPartitionValues());
    PartitionKey partitionKey2 = new PartitionKey(partitionError2.getPartitionValues());
    assertEquals(partitionKey1, partitionKey2);
    assertEquals(partitionKey1.hashCode(), partitionKey2.hashCode());
  }

  @Test
  public void testEqualsPartitionAndPartitionError() {
    List<String> values1 = Lists.newArrayList("value1", "value2");
    List<String> values2 = Lists.newArrayList("value1", "value2");
    Partition partition = TestObjects.getTestPartition("ns", "table", values1);
    PartitionError partitionError = TestObjects.getPartitionError(values2, new RuntimeException("foo"));
    PartitionKey partitionKey1 = new PartitionKey(partition);
    PartitionKey partitionKey2 = new PartitionKey(partitionError.getPartitionValues());
    assertEquals(partitionKey1, partitionKey2);
    assertEquals(partitionKey1.hashCode(), partitionKey2.hashCode());
  }

  @Test
  public void testEqualsNull() {
    List<String> values = Lists.newArrayList("value1", "value2");
    Partition partition = TestObjects.getTestPartition("ns", "table", values);
    PartitionKey partitionKey = new PartitionKey(partition);
    assertFalse(partitionKey.equals(null));
  }

  @Test
  public void testGetValues() {
    List<String> values = Lists.newArrayList("value1", "value2");
    Partition partition = TestObjects.getTestPartition("ns", "table", values);
    PartitionKey partitionKey1 = new PartitionKey(partition);
    assertEquals(Lists.newArrayList(values), partitionKey1.getValues());

    PartitionError partitionError = TestObjects.getPartitionError(values, new RuntimeException("foo"));
    PartitionKey partitionKey2 = new PartitionKey(partitionError.getPartitionValues());
    assertEquals(Lists.newArrayList(values), partitionKey2.getValues());
  }

}