package org.apache.streampark.gateway.results;

import lombok.Data;

/** Condition of result query. */
@Data
public class ResultQueryCondition {
  public FetchOrientation orientation;

  public long token;
  public int maxRows;
}
