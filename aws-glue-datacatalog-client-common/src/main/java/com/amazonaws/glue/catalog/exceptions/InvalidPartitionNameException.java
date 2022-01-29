package com.amazonaws.glue.catalog.exceptions;

public class InvalidPartitionNameException extends RuntimeException {

  public InvalidPartitionNameException(String message) {
    super(message);
  }

  public InvalidPartitionNameException(String message, Throwable cause) {
    super(message, cause);
  }
}
