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
            insertData();
            deleteDatabase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl())) {
            String query = "CREATE DATABASE DB_USER";
            executeStatement(connection, query);
        }
    }

    private static void createTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl("DB_USER"))) {
            String query = "CREATE TABLE T_USER (id INTEGER not NULL, age INTEGER not NULL, PRIMARY KEY ( id ))";
            executeStatement(connection, query);
        }
    }

    private static void insertData() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl("DB_USER"))) {
            String query = "INSERT INTO T_USER (id, age) VALUES (1, 12)";
            executeStatement(connection, query);
            query = "INSERT INTO T_USER (id, age) VALUES (2, 13)";
            executeStatement(connection, query);
        }
    }

    private static void deleteDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl())) {
            String query = "DROP DATABASE DB_USER";
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
