import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.sql.ResultSet;
import java.sql.SQLException;
import common.HttpResponse;
import common.ConfigUtils;
import common.DbUtils;
import common.HttpUtils;

public class UserService {
    static HttpServer server;
    final static String dbPath = "src/db/user.db";
    static String dbIp;
    static int dbPort;
    static String dbName = "ecommerce";
    static String dbPassword = "password";

    public static void main(String[] args) throws IOException {
        // loadUsers(dbPath);
        String configFileName = ConfigUtils.loadConfigFile(args[0]);
        dbIp = ConfigUtils.getIp("Database", configFileName);
        dbPort = ConfigUtils.getPort("Database", configFileName);

        int port = ConfigUtils.getPort("UserService", configFileName);
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(512)); // Adjust the pool size as needed
        server.createContext("/user", new UserPostHandler());
        server.createContext("/user/", new UserGetHandler());
        server.createContext("/shutdown", new ShutdownHandler());
        server.createContext("/wipe", new WipeHandler());
        server.start();
    }

    static class User {
        int id;
        String username, email, password;

        User(int id, String username, String email, String password) {
            this.id = id;
            this.username = username;
            this.email = email;
            this.password = password;
        }   
    }

    static class UserPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // check if content type is json
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType == null || !contentType.equals("application/json")) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // check if body is valid json
            String body = HttpUtils.getRequestBody(exchange);
            Map<String, String> json = HttpUtils.stringToJSON(body);
            if (json == null) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;                 
            }

            // parse command and user data
            String command = json.get("command");
            User userData = jsonToUser(json);
            if (userData == null) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // process user command and send response
            HttpResponse response = userCommand(command, userData);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);
        }
    }

    static class UserGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-GET requests
            if (!"GET".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // parse id
            int id;
            String []tokens = exchange.getRequestURI().toString().split("/");
            try {
                id = Integer.parseInt(tokens[2]);
            } catch (NumberFormatException e) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // send response json
            HttpResponse response = getUser(id);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);
        }
    }

    static class ShutdownHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }
            // storeUsers(dbPath);
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
            server.stop(0);
            System.exit(0);               
        }
    }

    static class WipeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }
            // wipeDB(dbPath);
            // db.clear();
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
        }
    }

    private static HttpResponse userCommand(String command, User userData) {
        if ("create".equals(command)) {
            return createUser(userData);
        } else if ("update".equals(command)) {
            return updateUser(userData);
        } else if ("delete".equals(command)) {
            return deleteUser(userData);
        }

        // service not found
        return new HttpResponse(404, "{}");
    }

    private static User jsonToUser(Map<String, String> json) {
        int id;
        try {
            id = Integer.parseInt(json.get("id"));
        } catch (NumberFormatException e) {
            return null;
        }
    
        String username = json.get("username");
        String email = json.get("email");
        String password = json.get("password");
        String hash;
        
        if (password != null) {
            hash = sha256(password);
        } else {
            hash = null;
        }

        User user = new User(id, username, email, hash);
        return user;
    }

    private static HttpResponse getUser(int id) {
        String sql = "SELECT id, username, email, password FROM users WHERE id = ?";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            statement.setInt(1, id);

            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return new HttpResponse(200, getUserJsonBuilder(result));
                } else {
                    return new HttpResponse(404, "{}");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HttpResponse(500, "{}");
    }

    private static String getUserJsonBuilder(ResultSet result) throws SQLException{
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":").append(result.getInt("id")).append(",");
        json.append("\"username\":\"").append(result.getString("username")).append("\",");
        json.append("\"email\":\"").append(result.getString("email")).append("\",");
        json.append("\"password\":\"").append(result.getString("password").toUpperCase()).append("\"");
        json.append("}");
        return json.toString();
    }


    private static HttpResponse createUser(User userData) {
        if (checkUserExistence(userData.id)) {
            return new HttpResponse(409, "{}");
        }
        if (userData.username == null || userData.email == null || userData.password == null) {
            return new HttpResponse(400, "{}");
        }
        if (userData.username.equals("") || userData.email.equals("") || userData.password.equals(sha256(""))) {
            return new HttpResponse(400, "{}");
        }    
        if (!validEmail(userData.email)) {
            return new HttpResponse(400, "{}");
        }        
        insertUser(userData);
        return new HttpResponse(200, userToJson(userData));
    }

    private static boolean checkUserExistence(int id) {
        String sql = "SELECT 1 FROM users WHERE id = ? LIMIT 1";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            statement.setInt(1, id);

            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }    
        return false;
    }

    private static HttpResponse updateUser(User userData) {
        // check user existence
        if (!checkUserExistence(userData.id)) {
            return new HttpResponse(404, "{}");
        }

        // build update command 
        StringBuilder sql = new StringBuilder("UPDATE users SET ");
        boolean usernameExist=false, emailExist=false, passwordExist=false;
        boolean first = true;
        if (userData.username != null) {
            if (userData.username.equals("")) {
                return new HttpResponse(400, "{}");
            }
            sql.append("username = ?");
            first = false;
            usernameExist = true;
        }
        if (userData.email != null) {
            if (userData.email.equals("") || !validEmail(userData.email)) {
                return new HttpResponse(400, "{}");
            }
            if (!first) {
                sql.append(", ");
            }
            sql.append("email = ?");
            first = false;
            emailExist = true;
        }
        if (userData.password != null) {
            if (userData.password.equals(sha256(""))) {
                return new HttpResponse(400, "{}");
            }
            if (!first) {
                sql.append(", ");
            }
            sql.append("password = ?");
            first = false;
            passwordExist = true;
        }
        if (!usernameExist && !emailExist && !passwordExist) {
            return getUser(userData.id); 
        }
        sql.append(" WHERE id = ? ");
        sql.append("RETURNING id, username, email, password ");

        // update user data
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql.toString())) {
            int i = 1;
            if (usernameExist) {
                statement.setString(i, userData.username);
                i++;
            }
            if (emailExist) {
                statement.setString(i, userData.email);
                i++;
            }
            if (passwordExist) {
                statement.setString(i, userData.password);
                i++;
            }
            statement.setInt(i, userData.id);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return new HttpResponse(200, getUserJsonBuilder(result));
                } else {
                    return new HttpResponse(500, "{}");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }        
        return new HttpResponse(500, "{}");
    }

    private static HttpResponse deleteUser(User userData) {
        // check if all fields exist
        if (userData.username == null || userData.email == null || userData.password == null) {
            return new HttpResponse(400, "{}");
        }

        // check user existence
        if (!checkUserExistence(userData.id)) {
            return new HttpResponse(404, "{}");
        }

        // delete if fields correspond
        String sql = "DELETE FROM users WHERE id = ? AND username = ? AND email = ? AND password = ?";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            
            statement.setInt(1, userData.id);
            statement.setString(2, userData.username);
            statement.setString(3, userData.email);
            statement.setString(4, userData.password);

            int deletedRows = statement.executeUpdate();

            if (deletedRows == 1) {
                return new HttpResponse(200, "{}");
            }

            return new HttpResponse(401, "{}");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return new HttpResponse(500, "{}");
    }

    private static String sha256(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
            StringBuilder string = new StringBuilder();
            for (byte b:hash) {
                string.append(String.format("%02x", b));
            }
            return string.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String userToJson(User user) {
        String json = "{" + "\"id\":" + user.id + "," + 
            "\"username\":" + "\"" + user.username + "\"" + "," +
            "\"email\":" + "\"" + user.email + "\"" + "," +
            "\"password\":" + "\"" + user.password.toUpperCase() + "\"" +
            "}";
        return json;
    }

    private static boolean validEmail(String email) {
        return email.contains("@");
    }

    private static void insertUser(User user) {
        String sql = "INSERT INTO users (id, username, email, password) VALUES (?, ?, ?, ?)";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            
            statement.setInt(1, user.id);
            statement.setString(2, user.username);
            statement.setString(3, user.email);
            statement.setString(4, user.password);

            statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

