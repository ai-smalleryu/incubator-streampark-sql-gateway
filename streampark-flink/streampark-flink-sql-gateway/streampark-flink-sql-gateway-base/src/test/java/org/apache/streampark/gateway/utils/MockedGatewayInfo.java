package org.apache.streampark.gateway.utils;

import org.apache.streampark.gateway.results.GatewayInfo;

import static org.junit.jupiter.api.Assertions.*;

/** Mocked implementation of {@link GatewayInfo}. */
public class MockedGatewayInfo implements GatewayInfo {

  @Override
  public String getEndpointType() {
    return "Mocked endpoint type";
  }

  @Override
  public String getVersion() {
    return "Mocked version";
  }
}
