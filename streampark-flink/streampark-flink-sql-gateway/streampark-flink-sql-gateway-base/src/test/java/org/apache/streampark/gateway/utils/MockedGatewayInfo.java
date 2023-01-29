package org.apache.streampark.gateway.utils;

import org.apache.streampark.gateway.results.GatewayInfo;

/** Mocked implementation of {@link GatewayInfo}. */
public class MockedGatewayInfo implements GatewayInfo {

  @Override
  public String getServiceType() {
    return "Mocked service type";
  }

  @Override
  public String getVersion() {
    return "Mocked version";
  }
}
