package com.azzayork.ignite.jdbc;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;

public class JdbcHelper {

    Connection conn;

    @SneakyThrows()
    public JdbcHelper() {

        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Open JDBC connection.
        conn = DriverManager.getConnection("com.azzayork.ignite.jdbc:ignite:thin://127.0.0.1/");
    }

    @SneakyThrows
    public void createCityTable() {

        // Create database tables.
        try (Statement stmt = conn.createStatement()) {

            // Create table based on REPLICATED template.
            stmt.executeUpdate("CREATE TABLE City (" +
                    " id LONG PRIMARY KEY, name VARCHAR) " +
                    " WITH \"template=replicated\"");

            // Create an index on the City table.
            stmt.executeUpdate("CREATE INDEX idx_city_name ON City (name)");
        }
    }

    @SneakyThrows
    public void createPersonTable() {

        // Create database tables.
        try (Statement stmt = conn.createStatement()) {

            // Create table based on PARTITIONED template with one backup.
            stmt.executeUpdate("CREATE TABLE Person (" +
                    " id LONG, name VARCHAR, city_id LONG, " +
                    " PRIMARY KEY (id, city_id)) " +
                    " WITH \"backups=1, affinityKey=city_id\"");

            // Create an index on the Person table.
            stmt.executeUpdate("CREATE INDEX idx_person_name ON Person (name)");
        }
    }

    @SneakyThrows
    public void dropTable(String tableName) {

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
