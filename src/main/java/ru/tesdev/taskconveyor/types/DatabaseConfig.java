package ru.tesdev.taskconveyor.types;

public class DatabaseConfig {
    private final String connectUrl;
    private final String user;
    private final String password;

    public DatabaseConfig(
            String connectUrl,
            String user,
            String password
    ) {
        this.connectUrl = connectUrl;
        this.user = user;
        this.password = password;
    }

    public String getConnectUrl() {
        return connectUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
