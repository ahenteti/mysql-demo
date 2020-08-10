package io.github.ahenteti.mysqldemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLDemo {

    public static void main(String[] args) {
        try {
            createDatabase();
            createTable();
            deleteDatabase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl())) {
            String query = "CREATE DATABASE DATABASE_NAME";
            executeStatement(connection, query);
        }
    }

    private static void createTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl("DATABASE_NAME"))) {
            String query = "CREATE TABLE TABLE_NAME (id INTEGER not NULL, PRIMARY KEY ( id ))";
            executeStatement(connection, query);
        }
    }

    private static void deleteDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl())) {
            String query = "DROP DATABASE DATABASE_NAME";
            executeStatement(connection, query);
        }
    }

    private static String getDbUrl() {
        return getDbUrl("");
    }

    private static String getDbUrl(String database) {
        return String.format("jdbc:mysql://localhost:3306/%s?user=root&password=mysql&serverTimezone=UTC", database);
    }

    private static void executeStatement(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        }
    }
}
