import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.sql.SQLException;
import common.HttpResponse;
import common.DbUtils;
import common.HttpUtils;
import common.JsonUtils;

public class UserService {
    // service info
    final static int port = 14001;
    final static int flushTime = 1;
    static HttpServer server;
    static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // database info
    final static int poolSize = 4;
    static BlockingQueue<Connection> dbPool = new ArrayBlockingQueue<>(poolSize);
    static JsonUtils.Service dbService;
    static String dbName = "userdb";
    static String dbPassword = "password";

    // cache
    static Map<Integer, User> cache = new ConcurrentHashMap<>();
    static Map<Integer, User> dirtyUsers = new ConcurrentHashMap<>();
    static Set<Integer> deletedUsers = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) throws Exception {
        // database init
        JsonUtils.Config config = new Gson().fromJson(
            new FileReader(args[0]), 
            JsonUtils.Config.class
        );
        JsonUtils.ServiceConfig dbConfig = config.UserDatabase;
        dbService = new JsonUtils.Service("Database", dbConfig.ip, dbConfig.port);
        initDbPool();
        
        // periodic flush thread
        Runnable task = () -> {
            try {
                flushToDB();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        scheduler.scheduleAtFixedRate(task, flushTime, flushTime, TimeUnit.SECONDS);

        // service init
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(256)); 
        server.createContext("/user", new UserPostHandler());
        server.createContext("/user/", new UserGetHandler());
        server.createContext("/livecheck", new LiveCheckHandler());
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

            // response
            User user = getUser(id);
            if (user == null) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }
            HttpUtils.sendJsonResponse(exchange, 200, userToJson(user));
        }
    }

    static class LiveCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-GET requests
            if (!"GET".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // live reply
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
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

    private static User getUser(int id) {
        // get user from cache
        User user = dirtyUsers.get(id);
        if (user != null) return user;
        user = cache.get(id);
        if (user != null) return user;

        // get user from db
        String sql = "SELECT id, username, email, password FROM users WHERE id = ?";
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            try (var statement = dbConnection.prepareStatement(sql)) {
                statement.setInt(1, id);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        user = buildUser(result);
                        cache.put(id, user);
                        return user;
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dbConnection != null) {
                try {
                    dbPool.put(dbConnection);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }  
        return null;     
    }

    private static User buildUser(ResultSet result) throws SQLException {
        return new User(
            result.getInt("id"), 
            result.getString("username"), 
            result.getString("email"), 
            result.getString("password"));
    }

    private static HttpResponse userCommand(String command, User userData) {
        if ("create".equals(command)) {
            return createUser(userData);
        } else if ("update".equals(command)) {
            return updateUser(userData);
        } else if ("delete".equals(command)) {
            return deleteUser(userData);
        }
        return new HttpResponse(404, "{}");
    }

    private static User jsonToUser(Map<String, String> json) {
        try {
            int id = Integer.parseInt(json.get("id"));
            String username = json.get("username");
            String email = json.get("email");
            String password = json.get("password");
            String hash;
            if (password != null) {
                hash = sha256(password);
            } else {
                hash = null;
            }
            return new User(id, username, email, hash);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static HttpResponse createUser(User userData) {
        if (getUser(userData.id) != null) {
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
        deletedUsers.remove(userData.id);
        cache.put(userData.id, userData);     
        dirtyUsers.put(userData.id, userData);
        return new HttpResponse(200, userToJson(userData));
    }

    private static HttpResponse updateUser(User userData) {
        User user = getUser(userData.id);
        // check if user exists
        if (user == null) {
            return new HttpResponse(404, "{}");
        }

        // update userdata
        if (userData.username != null) {
            if (userData.username.equals("")) {
                return new HttpResponse(400, "{}");
            }
            user.username = userData.username;
        }
        if (userData.email != null) {
            if (userData.email.equals("") || !validEmail(userData.email)) {
                return new HttpResponse(400, "{}");
            }
            user.email = userData.email;
        }
        if (userData.password != null) {
            if (userData.password.equals(sha256(""))) {
                return new HttpResponse(400, "{}");
            }
            user.password = userData.password;
        }
        dirtyUsers.put(userData.id, user);
        return new HttpResponse(200, userToJson(user));
    }

    private static HttpResponse deleteUser(User userData) {
        // check if all fields exist
        if (userData.username == null || userData.email == null || userData.password == null) {
            return new HttpResponse(400, "{}");
        }

        // check if user exists
        User user = getUser(userData.id);
        if (user == null) {
            return new HttpResponse(404, "{}");
        }

        // delete if fields correspond
        if (userData.username.equals(user.username) && userData.email.equals(user.email) && userData.password.equals(user.password)) {
            cache.remove(userData.id);
            dirtyUsers.remove(userData.id);
            deletedUsers.add(userData.id);
            return new HttpResponse(200, "{}");
        }
        return new HttpResponse(401, "{}");
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

    private static void insertUserToDB(User user) {
        String sql = """
            INSERT INTO users (id, username, email, password) 
            VALUES (?, ?, ?, ?) 
            ON CONFLICT (id)
            DO UPDATE SET
                username = EXCLUDED.username, 
                email = EXCLUDED.email, 
                password = EXCLUDED.password
            """;
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            try (var statement = dbConnection.prepareStatement(sql)) {
                statement.setInt(1, user.id);
                statement.setString(2, user.username);
                statement.setString(3, user.email);
                statement.setString(4, user.password);

                statement.executeUpdate();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dbConnection != null) {
                try {
                    dbPool.put(dbConnection);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }
    }

    private static void deleteUserFromDB(int id) {
        String sql = "DELETE FROM users WHERE id = ?";
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            try (var statement = dbConnection.prepareStatement(sql)) {
                statement.setInt(1, id);
                statement.executeUpdate();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dbConnection != null) {
                try {
                    dbPool.put(dbConnection);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }
    }

    private static void flushToDB() {
        for (Map.Entry<Integer, User> entry : dirtyUsers.entrySet()) {
            User user = entry.getValue();
            insertUserToDB(user);
            dirtyUsers.remove(entry.getKey());
        }

        for (Integer id : Set.copyOf(deletedUsers)) {
            deleteUserFromDB(id);
            deletedUsers.remove(id);
        }
    }

    private static void initDbPool() throws SQLException{
        for (int i=0;i<poolSize;i++) {
            Connection dbConnection = DbUtils.getDBConnection(
                dbService.ip, dbService.port, dbName, dbPassword
            );
            dbPool.add(dbConnection);
        }
    }    
}