package org.apache.streampark.gateway.utils;

import org.apache.streampark.common.conf.ConfigOption;
import org.apache.streampark.gateway.factories.SqlGatewayServiceFactory;
import org.apache.streampark.gateway.service.SqlGatewayService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Factory for {@link SqlGatewayService}. */
public class FakeSqlGatewayServiceFactory implements SqlGatewayServiceFactory {

  public static final ConfigOption<String> HOST =
      new ConfigOption<String>(
          "host",
          "localhost",
          true,
          String.class,
          "The host of the Fake SQL gateway service",
          null,
          null,
          null);
  public static final ConfigOption<Integer> PORT =
      new ConfigOption<Integer>(
          "host",
          8080,
          true,
          Integer.class,
          "The port of the Fake SQL gateway service",
          null,
          null,
          null);
  public static final ConfigOption<Integer> DESCRIPTION =
      new ConfigOption<Integer>(
          "host",
          8080,
          false,
          Integer.class,
          "The port of the Fake SQL gateway service",
          null,
          null,
          null);

  @Override
  public String factoryIdentifier() {
    return "fake";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(HOST);
    options.add(PORT);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.singleton(DESCRIPTION);
  }

  @Override
  public SqlGatewayService createSqlGatewayService(Context context) {
    return FakeSqlGatewayService.INSTANCE;
  }
}
