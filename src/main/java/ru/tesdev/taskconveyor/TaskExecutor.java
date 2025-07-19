package ru.tesdev.taskconveyor;

import org.slf4j.Logger;

public interface TaskExecutor {
    void execute(Task task, Logger logger);
}
