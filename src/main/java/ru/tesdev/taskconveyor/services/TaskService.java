package ru.tesdev.taskconveyor.services;

import ru.tesdev.taskconveyor.types.DatabaseConfig;
import ru.tesdev.taskconveyor.exceptions.TaskConveyorException;

import java.sql.*;
import java.util.UUID;

public class TaskService {
    private final DatabaseConfig dbConfig;
    private final LoggingService loggingService;

    public TaskService(DatabaseConfig dbConfig, LoggingService loggingService) {
        this.dbConfig = dbConfig;
        this.loggingService = loggingService;
    }

    public void updateStatus(UUID uuid, ru.tesdev.taskconveyor.types.TaskStatus status) throws TaskConveyorException {
        try (Connection connection = DriverManager.getConnection(dbConfig.getConnectUrl(),
                dbConfig.getUser(), dbConfig.getPassword())) {
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE taskconveyor.tc_tasks SET status = ?::taskconveyor.task_status WHERE uuid = ?::uuid AND locked = false")) {
                stmt.setString(1, status.name().toLowerCase());
                stmt.setString(2, uuid.toString());
                stmt.executeUpdate();
            }
            loggingService.markTask(uuid, status);
        } catch (SQLException e) {
            throw new TaskConveyorException(e.getMessage());
        }
    }

    public void setLock(UUID uuid, boolean lock) throws TaskConveyorException {
        try (Connection connection = DriverManager.getConnection(dbConfig.getConnectUrl(),
                dbConfig.getUser(), dbConfig.getPassword())) {
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE taskconveyor.tc_tasks SET locked = ? WHERE uuid = ?::uuid")) {
                stmt.setBoolean(1, lock);
                stmt.setString(2, uuid.toString());
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new TaskConveyorException(e.getMessage());
            }
        } catch (SQLException e) {
            throw new TaskConveyorException(e.getMessage());
        }
    }
}
