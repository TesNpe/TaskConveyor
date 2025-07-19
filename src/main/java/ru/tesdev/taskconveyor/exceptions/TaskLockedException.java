package ru.tesdev.taskconveyor.exceptions;

public class TaskLockedException extends TaskConveyorException {
    public TaskLockedException(String message) {
        super(message);
    }
}
