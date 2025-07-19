package ru.tesdev.taskconveyor;

import org.slf4j.Logger;

import java.util.UUID;

public abstract class EventsExecutor {
    public void onPollingStart(Logger logger, TaskConveyor conveyor) { }
    public void onPollingStop(Logger logger, TaskConveyor conveyor) { }
    public void onTaskEnd(Task task, Logger logger) { }
    public void onPollCause(UUID taskUUID, Logger logger, Throwable exception) {
        logger.error("Poll Cause: {}", exception.getMessage());
    }
}
