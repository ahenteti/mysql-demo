package io.github.ahenteti.mysqldemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLDemo {

    public static final String DB_NAME = "DB_USER";

    public static void main(String[] args) {
        try {
            createDatabase();
            createTable();
            insertData();
            createGetUsersCountProcedure();
            createGetUsersAverageAgeProcedure();
            deleteDatabase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl())) {
            String query = "CREATE DATABASE " + DB_NAME;
            executeAndUpdateStatement(connection, query);
        }
    }

    private static void createTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl(DB_NAME))) {
            String query = "CREATE TABLE T_USERS (id INTEGER not NULL, age INTEGER not NULL, PRIMARY KEY ( id ))";
            executeAndUpdateStatement(connection, query);
        }
    }

    private static void insertData() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl(DB_NAME))) {
            String query = "INSERT INTO T_USERS (id, age) VALUES (1, 12)";
            executeAndUpdateStatement(connection, query);
            query = "INSERT INTO T_USERS (id, age) VALUES (2, 13)";
            executeAndUpdateStatement(connection, query);
        }
    }

    private static void createGetUsersCountProcedure() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl(DB_NAME))) {
            // @formatter:off
            String query = ""
                + "CREATE PROCEDURE GetUsersCount(OUT counter INT)\n"
                + "BEGIN\n"
                + "    SELECT count(*) INTO counter\n"
                + "    FROM T_USERS;\n"
                + "END";
            // @formatter:on
            executeStatement(connection, query);
        }
    }

    private static void createGetUsersAverageAgeProcedure() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl(DB_NAME))) {
            // @formatter:off
            String query = ""
                + "CREATE PROCEDURE GetUsersAverageAge(OUT counter INT)\n"
                + "BEGIN\n"
                + "    SELECT avg(age) INTO counter\n" 
                + "    FROM T_USERS;\n"
                + "END";
            // @formatter:on
            executeStatement(connection, query);
        }
    }

    private static void deleteDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbUrl())) {
            String query = "DROP DATABASE " + DB_NAME;
            executeAndUpdateStatement(connection, query);
        }
    }

    private static String getDbUrl() {
        return getDbUrl("");
    }

    private static String getDbUrl(String database) {
        return String.format("jdbc:mysql://localhost:3306/%s?user=root&password=mysql&serverTimezone=UTC", database);
    }

    private static void executeAndUpdateStatement(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        }
    }

    private static void executeStatement(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
        }
    }
}
