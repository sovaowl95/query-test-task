package org.query.calc.dto;

public class Row {
  private final double key;
  private final double value;

  public Row(double key, double value) {
    this.key = key;
    this.value = value;
  }

  public double getKey() {
    return key;
  }

  public double getValue() {
    return value;
  }
}
