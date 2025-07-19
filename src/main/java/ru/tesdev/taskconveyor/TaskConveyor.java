package ru.tesdev.taskconveyor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import ru.tesdev.taskconveyor.types.TaskStatus;
import ru.tesdev.taskconveyor.types.DatabaseConfig;
import ru.tesdev.taskconveyor.services.TaskService;
import ru.tesdev.taskconveyor.services.LoggingService;
import ru.tesdev.taskconveyor.exceptions.NoValidTaskStatusException;
import ru.tesdev.taskconveyor.exceptions.PollException;
import ru.tesdev.taskconveyor.exceptions.TaskConveyorException;
import ru.tesdev.taskconveyor.exceptions.TaskPayloadException;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class TaskConveyor {
    private final String handlerName;
    private final DatabaseConfig databaseConfig;
    private final int workerThreads;

    private final Map<String, TaskExecutor> executors = new HashMap<>();
    private final TaskService service;
    private final LoggingService logging;
    private EventsExecutor taskEventExecutor = new EventsExecutor() { };
    private ExecutorService taskPull;

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean autoDone = new AtomicBoolean(false);
    private final AtomicReference<Logger> logger = new AtomicReference<>(LoggerFactory.getLogger("TaskConveyor"));

    public TaskConveyor(
            String handlerName,
            DatabaseConfig databaseConfig,
            Path loggingFolder,
            int workerThreads,
            boolean autoDone
    ) {
        this.handlerName = handlerName;
        this.databaseConfig = databaseConfig;
        this.logging = new LoggingService(loggingFolder);
        this.service = new TaskService(databaseConfig, logging);
        this.workerThreads = workerThreads;
        this.autoDone.set(autoDone);
    }

    public void infinityPolling() {
        logging.newLogFile();
        enabled.set(true);
        if (workerThreads == -1) {
            taskPull = Executors.newCachedThreadPool(new TaskThreadFactory("TaskWorker"));
        } else {
            taskPull = Executors.newFixedThreadPool(workerThreads, new TaskThreadFactory("TaskWorker"));
        }
        new Thread(() -> {
            Thread.currentThread().setName("MainTaskConveyorThread");
            while (enabled.get()) {
                try {
                    poll();
                    Thread.sleep(1000);
                } catch (PollException e) {
                    logger.get().error("Unable to poll");
                    logger.get().error(e.getMessage());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        logging.pollingStarted();
        taskEventExecutor.onPollingStart(logger.get(), this);
    }

    public void stopPolling() {
        enabled.set(false);
        taskPull.shutdown();
        logging.pollingShutdown();
        taskEventExecutor.onPollingStop(logger.get(), this);
    }

    private void poll() throws PollException {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getConnectUrl(), databaseConfig.getUser(), databaseConfig.getPassword())) {
            try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM taskconveyor.tc_tasks WHERE status = 'new' AND locked = false AND (handler_name = ? OR handler_name = '*')")) {
                stmt.setString(1, handlerName);
                ResultSet rs = stmt.executeQuery();
                List<Task> tasks = new ArrayList<>();
                while (rs.next()) {
                    boolean isNormal = true;

                    int index = rs.getInt("index");
                    UUID uuid = UUID.fromString(rs.getString("uuid"));
                    String type = rs.getString("type");
                    long createdAt = rs.getLong("created_at");
                    String owner = rs.getString("owner");
                    String description = rs.getString("description");
                    boolean locked = rs.getBoolean("locked");

                    JsonNode payload = null;
                    try {
                        payload = processingPayload(rs.getString("payload"));
                    } catch (TaskPayloadException e) {
                        pollCause(uuid, "Error in format payload");
                        isNormal = false;
                    }

                    TaskStatus status = null;
                    try {
                        status = transformTaskStatus(rs.getString("status"));
                    } catch (NoValidTaskStatusException e) {
                        pollCause(uuid, String.format("No valid task status %s", rs.getString("status")));
                        isNormal = false;
                    }

                    if (isNormal) {
                        tasks.add(new Task(index, uuid, type, payload, status, createdAt, owner, description, locked, this.service));
                    }
                }
                executeTasks(tasks);
            }
        } catch (SQLException e) {
            throw new PollException(e.getMessage());
        }
    }

    private void pollCause(UUID uuid, String message) {
        try {
            service.updateStatus(uuid, TaskStatus.DENY);
            service.setLock(uuid, true);
        } catch (Exception e) {
            logger.get().error(e.getMessage());
        }

        taskEventExecutor.onPollCause(uuid, logger.get(), new PollException(message));
    }

    private JsonNode processingPayload(String raw) throws TaskPayloadException {
        try {
            return new ObjectMapper(new JsonFactory()).readTree(raw);
        } catch (JsonProcessingException e) {
            throw new TaskPayloadException(e.getMessage());
        }
    }

    private TaskStatus transformTaskStatus(String status) throws NoValidTaskStatusException {
        if (status == null) { throw new NoValidTaskStatusException("Status is null"); }
        try {
            return TaskStatus.valueOf(status.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new NoValidTaskStatusException(String.format("Status %s no valid", status));
        }
    }

    private void executeTask(Task task, TaskExecutor executor, Logger logger) {
        executor.execute(task, logger);
        try {
            if (autoDone.get() && task.getStatus() == TaskStatus.WORK && !task.isLocked()) {
                logger.debug("been done");
                task.done();
            } else if (task.getStatus() == TaskStatus.WORK && !task.isLocked()) {
                logger.debug("been deny");
                task.deny();
            } else {
                logger.debug("else");
                logger.debug("auto done: {}", autoDone.get());
                logger.debug("status: {}", task.getStatus().name());
                logger.debug("locked: {}", task.isLocked());
            }
        } catch (TaskConveyorException e) { }

        taskEventExecutor.onTaskEnd(task, logger);
    }

    private void executeTasks(List<Task> tasks) {
        for (Task task : tasks.stream().sorted(Comparator.comparingInt(Task::getIndex)).toList()) {
            if (executors.containsKey(task.getType())) {
                TaskExecutor executor = executors.get(task.getType());
                try {
                    if (enabled.get()) {
                        logging.executeTask(task);
                        task.setStatus(TaskStatus.WORK);
                        taskPull.submit(() -> executeTask(task, executor, logger.get()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    logging.unHandleType(task);
                    service.updateStatus(task.getId(), TaskStatus.DENY);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void registerType(String type, TaskExecutor executor) {
        executors.put(type, executor);
    }

    public void loadEventExecutor(EventsExecutor executor) {
        this.taskEventExecutor = executor;
    }

    public void prepareDataBase() throws TaskConveyorException {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getConnectUrl(), databaseConfig.getUser(), databaseConfig.getPassword())) {
            try (PreparedStatement stmt = connection.prepareStatement("""
                CREATE SCHEMA IF NOT EXISTS taskconveyor;
                SET search_path TO taskconveyor;
            
                -- enum type
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_status') THEN
                        CREATE TYPE task_status AS ENUM ('new', 'work', 'done', 'deny');
                    end if;
                END$$;
            
                CREATE TABLE IF NOT EXISTS TC_Tasks
                (
                    index           INTEGER         GENERATED ALWAYS AS IDENTITY,
                    uuid            UUID            DEFAULT gen_random_uuid(),
                    type            TEXT            NOT NULL,
                    payload         JSON            DEFAULT '{}'::json,
                    status          task_status     DEFAULT 'new',
                    created_at      BIGINT          DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    owner           TEXT            NOT NULL,
                    description     TEXT,
                    locked          BOOLEAN         DEFAULT FALSE,
                    handler_name      TEXT            NOT NULL
                );
            
                -- locked prevent update script
                CREATE OR REPLACE FUNCTION prevent_update_if_locked()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF OLD.locked = TRUE AND NEW.locked != FALSE THEN
                        RAISE EXCEPTION 'Row locked';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            
                -- trigger
                DROP TRIGGER IF EXISTS trg_prevent_update_if_locked ON TC_Tasks;
                CREATE TRIGGER trg_prevent_update_if_locked
                    BEFORE UPDATE ON TC_Tasks
                    FOR EACH ROW
                EXECUTE FUNCTION prevent_update_if_locked();
            """)) {
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new TaskConveyorException(e.getMessage());
        }
    }

    public String getHandlerName() {
        return handlerName;
    }
}
