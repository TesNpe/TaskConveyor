package ru.tesdev.taskconveyor;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class TaskThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger threadCount = new AtomicInteger(1);

    public TaskThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(@NotNull Runnable r) {
        int id = threadCount.getAndIncrement();

        Runnable wrapped = () -> {
            try {
                r.run();
            } finally {
                threadCount.getAndDecrement();
            }
        };

        Thread t = new Thread(wrapped);
        t.setName(poolName+"-" + id);
        return t;
    }
}
