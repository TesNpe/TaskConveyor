package ru.tesdev.taskconveyor;

import com.fasterxml.jackson.databind.JsonNode;
import ru.tesdev.taskconveyor.exceptions.TaskConveyorException;
import ru.tesdev.taskconveyor.services.TaskService;
import ru.tesdev.taskconveyor.types.TaskStatus;

import java.util.UUID;

public class Task {
    private final int index;
    private final UUID id;
    private final String type;
    private final JsonNode payload;
    private TaskStatus status;
    private final long createdAt;
    private final String owner;
    private final String description;
    private boolean locked;

    private final TaskService service;

    public Task(int index,UUID id, String type, JsonNode payload,
                TaskStatus status, long createdAt, String owner, String description, boolean locked,
                TaskService service) {
        this.index = index;
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
        this.owner = owner;
        this.description = description;
        this.locked = locked;
        this.service = service;
    }

    public void done() throws TaskConveyorException {
        service.updateStatus(this.id, TaskStatus.DONE);
        service.setLock(this.id, true);
        this.status = TaskStatus.DENY;
    }

    public void deny() throws TaskConveyorException {
        service.updateStatus(this.id, TaskStatus.DENY);
        service.setLock(this.id, true);
        this.status = TaskStatus.DENY;
    }

    public void unlock() throws TaskConveyorException {
        service.setLock(this.id, false);
        this.locked = false;
    }

    public void setStatus(TaskStatus status) throws TaskConveyorException {
        service.updateStatus(this.id, status);
        this.status = status;
    }

    public int getIndex() { return index; }
    public UUID getId() { return id; }
    public String getType() { return type; }
    public JsonNode getPayload() { return payload; }
    public TaskStatus getStatus() {return status; }
    public String getOwner() {return owner; }
    public long createdAt() { return createdAt; }
    public String getDescription() { return description; }
    public boolean isLocked() { return locked; }
    public void setLocked(boolean locked) { this.locked = locked; }
}
