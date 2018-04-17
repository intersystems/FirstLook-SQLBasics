package com.intersystems.firstlooks.sqlbasics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;

import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Random;

import java.util.Properties;


public class Main {

    public static void main(String[] args) {

        Connection dbConnection;

        String  URLroot             = "jdbc:IRIS://127.0.0.1:";
        String  URL;
        String  superserverPort     = null;

        String  schemaName          = "Sample";
        String  tableName           = "StockTrades";
        String  schemaAndTableName  = schemaName + "." + tableName;
        int     rowCount            = 50;
        String  tableCheckSQL       = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schemaName + "' AND  TABLE_NAME = '" + tableName + "'";

        StringBuilder builder = new StringBuilder();
        PreparedStatement pstmt;
        ResultSet rs;
        Random r;


        String userName = null;
        String password = null;
        LocalDateTime startTime;

        String customerID = null;
        String brokerID = null;
        String stockSymbol = null;


        Map<String,Float> brokerInfo = new HashMap<>();
        brokerInfo.put("B1000", 3.25f);
        brokerInfo.put("B1001", 6.25f);
        brokerInfo.put("B1002", 5.25f);
        brokerInfo.put("B1003", 4.25f);
        brokerInfo.put("B1004", 6.25f);
        brokerInfo.put("B1005", 5.25f);
        brokerInfo.put("B1006", 5.5f);
        brokerInfo.put("B1007", 5.5f);
        brokerInfo.put("B1008", 6.0f);
        brokerInfo.put("B1009", 5.0f);

        String sql;

        try {

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Enter your InterSystems IRIS system account user name.\n");
            userName = br.readLine().trim();

            System.out.println("Enter your InterSystems IRIS system account password.\n");

            if (System.console() != null) {
                char[] pword = System.console().readPassword();
                password = Arrays.toString(pword).trim();
            } else {
                password = br.readLine().trim();
            }

            System.out.println("Enter the port number for the InterSystems IRIS superserver.\n");
            superserverPort = br.readLine().trim();

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

            System.out.println("Opening TCP/IP connection now...");

            try {
                Properties p = new Properties();
                p.setProperty("user", userName);
                p.setProperty("password", password);
                p.setProperty("SharedMemory", "false");

                URL = URLroot + superserverPort + "/USER";
                System.out.println(URL);

                Class.forName("com.intersystems.jdbc.IRISDriver");
                dbConnection = DriverManager.getConnection(URL, p);
                System.out.println("we have a connection");
                System.out.println("Connected to InterSystems IRIS via JDBC: " + dbConnection.getClientInfo());

                pstmt = dbConnection.prepareStatement(tableCheckSQL);
                rs = pstmt.executeQuery();
                rs.next();

                if (rs.getInt(1) != 0) {

                    sql = "DROP TABLE " + schemaAndTableName;
                    pstmt = dbConnection.prepareStatement(sql);
                    pstmt.execute();
                    System.out.println(schemaAndTableName + " table drop statement submitted.");

                }

                sql = "CREATE TABLE " + schemaAndTableName + "  (CustomerID VARCHAR(32), BrokerID VARCHAR(32), StockSymbol VARCHAR(10), TransactionType VARCHAR(4), TransactionDate DATE, NbrShares INTEGER, PricePerShare DECIMAL(15,2), CommmissionRate DECIMAL(15,2))";
                pstmt = dbConnection.prepareStatement(sql);
                pstmt.execute();

                System.out.println(schemaAndTableName + " table creation statement submitted.");

                pstmt = dbConnection.prepareStatement(tableCheckSQL);
                rs = pstmt.executeQuery();

                rs.next();
                int count = rs.getInt(1);
                System.out.println("Query on table creation successful: size of result set is " + NumberFormat.getInstance().format(count) + "\n");

                System.out.println("Inserting " + NumberFormat.getInstance().format(rowCount) + " rows, please wait...");

               for (int j = 1; j < rowCount + 1; j++) {

                   builder.append("INSERT INTO ").append(schemaAndTableName).append(" (CustomerID, BrokerID, StockSymbol, TransactionType, TransactionDate, NbrShares, PricePerShare, CommmissionRate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

                  LocalDate startDate = LocalDate.of(2016, 1, 1); //start date
                    long start = startDate.toEpochDay();

                    LocalDate endDate = LocalDate.now(); //end date
                    long end = endDate.toEpochDay();

                    long randomEpochDay = ThreadLocalRandom.current().longs(start, end).findAny().getAsLong();

                   String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                   Random rnd = new Random();

                   StringBuilder sb = new StringBuilder( 4 );
                       for( int m = 0; m < 4; m++ ) {
                           sb.append(AB.charAt(rnd.nextInt(AB.length())));
                       };

                    stockSymbol = sb.toString();

                    r = new Random();

                    Integer x = 10000000 + r.nextInt(9000000);
                    customerID = "C" + x;

                List<String> brokerKeys = new ArrayList<String>(brokerInfo.keySet());
                    brokerID = brokerKeys.get(r.nextInt(brokerKeys.size()));
                    Float commission = brokerInfo.get(brokerID);

                    Float min = 5.00f;
                    Float max = 300.00f;


                    pstmt = dbConnection.prepareStatement(builder.toString());
                    pstmt.setString(1, customerID);
                    pstmt.setString(2, brokerID);
                    pstmt.setString(3, stockSymbol);
                    if (r.nextInt() % 2 == 0) {
                        pstmt.setString(4, "BUY");
                    }
                    else {
                        pstmt.setString(4, "SELL");
                    }

                    pstmt.setDate(5, java.sql.Date.valueOf(LocalDate.ofEpochDay(randomEpochDay)));
                    pstmt.setInt(6, 1000 + r.nextInt(9000));
                   pstmt.setFloat(7,min + r.nextFloat() * (max - min));
                   pstmt.setFloat(8, commission);


                   pstmt.execute();
                   builder.setLength(0);
                }


                rs.close();

                dbConnection.close();
                System.out.println("Closed TCP/IP connection: value of isClosed is " + String.valueOf(dbConnection.isClosed()) + "\n");
            } catch (SQLException | ClassNotFoundException e) {
                System.out.println(e.getMessage());
            }

    }
}
