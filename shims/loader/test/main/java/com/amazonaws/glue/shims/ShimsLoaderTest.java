package com.amazonaws.glue.shims;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest(HiveVersionInfo.class)
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
public class ShimsLoaderTest {

  private static final String HIVE_1_2_VERSION = "1.2.1";

  private static final String HIVE_3_VERSION = "3.1.3";

  @Before
  public void setup() throws ClassNotFoundException {
    mockStatic(HiveVersionInfo.class);
  }

  @Test
  public void hive3ShimsClassSupportsCorrectVersion() {
    assertTrue(AwsGlueHive3Shims.supportsVersion(HIVE_3_VERSION));
  }

  @Test
  public void hive1Dot2ShimsClassSupportsCorrectVersion() {
    assertTrue(AwsGlueSparkHiveShims.supportsVersion(HIVE_1_2_VERSION));
  }

  @Test
  public void returnsCorrectShimsImplementationForHive3() {
    assertGetsCorrectShimsClassForVersion(AwsGlueHive3Shims.class, HIVE_3_VERSION);
  }

  @Test
  public void returnsCorrectShimsImplementationForHive1Dot2() {
    assertGetsCorrectShimsClassForVersion(AwsGlueSparkHiveShims.class, HIVE_1_2_VERSION);
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenSupportingClassDoesNotExist() {
    when(HiveVersionInfo.getShortVersion()).thenReturn("this.is.not.a.real.hive.version");
    ShimsLoader.clearShimClass();
    ShimsLoader.getHiveShims();
  }

  private void assertGetsCorrectShimsClassForVersion(Class expectedClass, String version) {
    when(HiveVersionInfo.getShortVersion()).thenReturn(version);
    AwsGlueHiveShims shims = ShimsLoader.getHiveShims();
    assertTrue(expectedClass.isInstance(shims));
    ShimsLoader.clearShimClass();
  }
}
