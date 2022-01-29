package com.amazonaws.glue.catalog.exceptions;

public class LakeFormationException extends RuntimeException {

  public LakeFormationException(String message) {
    super(message);
  }

  public LakeFormationException(String message, Throwable cause) {
    super(message, cause);
  }
}
