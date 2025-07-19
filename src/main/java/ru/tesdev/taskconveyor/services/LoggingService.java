package ru.tesdev.taskconveyor.services;

import org.jetbrains.annotations.Nullable;
import ru.tesdev.taskconveyor.Task;
import ru.tesdev.taskconveyor.types.TaskStatus;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class LoggingService {
    private final @Nullable Path loggingFolder;
    private String currentLogFile;

    public LoggingService(@Nullable Path loggingFolder) {
        this.loggingFolder = loggingFolder;
    }

    public void newLogFile() {
        LocalDateTime now = LocalDateTime.now();
        String formatted = now.format(DateTimeFormatter.ofPattern("HH-mm-ss-SSS_dd-MM-yyyy"));
        this.currentLogFile = formatted+".log";
        if (loggingFolder != null) {
            try {
                loggingFolder.toFile().mkdirs();
                Files.createFile(loggingFolder.resolve(currentLogFile));
            } catch (Exception e) { }
        }
    }

    public synchronized void executeTask(Task task) {
        if (loggingFolder != null) {
            try (FileWriter fw = new FileWriter(loggingFolder.resolve(currentLogFile).toFile(), true)) {
                LocalDateTime now = LocalDateTime.now();
                String line = String.format("[%s] (%s) Execute task %s Type %s", now.format(DateTimeFormatter.ofPattern("HH:mm:ss:SSS")), Thread.currentThread().getName(), task.getId().toString(), task.getType());
                fw.write(line+"\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void markTask(UUID uuid, TaskStatus mark) {
        if (loggingFolder != null) {
            try (FileWriter fw = new FileWriter(loggingFolder.resolve(currentLogFile).toFile(), true)) {
                LocalDateTime now = LocalDateTime.now();
                String line = String.format("[%s] (%s) Task %s marked as %s", now.format(DateTimeFormatter.ofPattern("HH:mm:ss:SSS")), Thread.currentThread().getName(), uuid, mark.name());
                fw.write(line+"\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void pollingStarted() {
        if (loggingFolder != null) {
            try (FileWriter fw = new FileWriter(loggingFolder.resolve(currentLogFile).toFile(), true)) {
                LocalDateTime now = LocalDateTime.now();
                String line = String.format("[%s] (%s) Polling started", now.format(DateTimeFormatter.ofPattern("HH:mm:ss:SSS")), Thread.currentThread().getName());
                fw.write(line + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void pollingShutdown() {
        if (loggingFolder != null) {
            try (FileWriter fw = new FileWriter(loggingFolder.resolve(currentLogFile).toFile(), true)) {
                LocalDateTime now = LocalDateTime.now();
                String line = String.format("[%s] (%s) Polling shutdown", now.format(DateTimeFormatter.ofPattern("HH:mm:ss:SSS")), Thread.currentThread().getName());
                fw.write(line + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void unHandleType(Task task) {
        if (loggingFolder != null) {
            try (FileWriter fw = new FileWriter(loggingFolder.resolve(currentLogFile).toFile(), true)) {
                LocalDateTime now = LocalDateTime.now();
                String line = String.format("[%s] (%s) Unhandled type %s in task %s", now.format(DateTimeFormatter.ofPattern("HH:mm:ss:SSS")), Thread.currentThread().getName(), task.getType(), task.getId().toString());
                fw.write(line + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
