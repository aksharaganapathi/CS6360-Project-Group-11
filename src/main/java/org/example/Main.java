package org.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {
    // PostgreSQL connection string
    static String pgUrl = "jdbc:postgresql://localhost:5432/test";

    // MongoDB connection string
    static String mongoUri = "mongodb://localhost:27017";

    public static void main(String[] args) {
        // Test PostgreSQL connection
        try (Connection conn = DriverManager.getConnection(pgUrl, "postgres", "test987")) {
            System.out.println("PostgreSQL connected successfully");
        } catch (SQLException e) {
            e.printStackTrace();
        }

// Test MongoDB connection
        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
            MongoDatabase database = mongoClient.getDatabase("test");
            System.out.println("MongoDB connected successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}