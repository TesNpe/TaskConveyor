package ru.tesdev.taskconveyor.exceptions;

public class NoValidTaskStatusException extends TaskConveyorException {
  public NoValidTaskStatusException(String message) {
    super(message);
  }
}
