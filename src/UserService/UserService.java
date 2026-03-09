import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner; 
import java.io.FileWriter;

public class UserService {
    // data base
    static Map<Integer, User> db = new ConcurrentHashMap<>();
    static HttpServer server;
    final static String dbPath = "src/db/user.db";

    public static void main(String[] args) throws IOException {
        loadUsers(dbPath);
        int port = getPort("UserService", args[0]);
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(20)); // Adjust the pool size as needed
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
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = getRequestBody(exchange);
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                if (contentType == null || !contentType.equals("application/json")) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();
                    return;
                }
                Map<String, String> json = stringToJSON(body);
                if (json == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();   
                    return;                 
                }

                // command types
                String command = json.get("command");
                User userData = jsonToUser(json);
                if (userData == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();
                    return;
                }
                int status;
                if ("create".equals(command)) {
                    status = createUser(userData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    if (status == 200) {
                        sendResponse(exchange, userToJson(db.get(userData.id)));
                    } else {
                        sendResponse(exchange, "{}");
                    }
                } else if ("update".equals(command)) {
                    status = updateUser(userData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    if (status == 200) {
                        sendResponse(exchange, userToJson(db.get(userData.id)));
                    } else {
                        sendResponse(exchange, "{}");
                    }
                } else if ("delete".equals(command)) {
                    status = deleteUser(userData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    if (status == 200) {
                        sendResponse(exchange, "{}");
                    } else {
                        sendResponse(exchange, "{}");
                    }
                
                // service not found
                } else {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, 0);
                    sendResponse(exchange, "{}");
                }
            } else {
                // Send a 405 Method Not Allowed response for non-POST requests
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
                sendResponse(exchange, "{}");
            }
            exchange.close();
            return;
        }

        private static String getRequestBody(HttpExchange exchange) throws IOException {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                StringBuilder requestBody = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    requestBody.append(line);
                }
                return requestBody.toString();
            }
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


        private static Integer createUser(User userData) {
            if (db.get(userData.id) != null) {
                return 409;
            }
            if (userData.username == null || userData.email == null || userData.password == null) {
                return 400;
            }
            if (userData.username.equals("") || userData.email.equals("") || userData.password.equals(sha256(""))) {
                return 400;
            }    
            if (!validEmail(userData.email)) {
                return 400;
            }        
            db.put(userData.id, userData);
            return 200;
        }

        private static Integer updateUser(User userData) {
            User user = db.get(userData.id);

            // check if user exists
            if (user == null) {
                return 404;
            }

            // update userdata
            if (userData.username != null) {
                if (userData.username.equals("")) {
                    return 400;
                }
                user.username = userData.username;
            }
            if (userData.email != null) {
                if (userData.email.equals("") || !validEmail(userData.email)) {
                    return 400;
                }
                user.email = userData.email;
            }
            if (userData.password != null) {
                if (userData.password.equals(sha256(""))) {
                    return 400;
                }
                user.password = userData.password;
            }
            return 200;
        }

        private static Integer deleteUser(User userData) {
            // check if all fields exist
            if (userData.username == null || userData.email == null || userData.password == null) {
                return 400;
            }

            // check if user exists
            User user = db.get(userData.id);
            if (user == null) {
                return 404;
            }

            // delete if fields correspond
            if (userData.username.equals(user.username) && userData.email.equals(user.email) && userData.password.equals(user.password)) {
                db.remove(userData.id);
                return 200;
            }
            return 401;
        }

        public static String sha256(String password) {
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

        public static Map<String, String> stringToJSON(String str) {
            Map<String, String> json = new HashMap<>();

            // check if string is a valid json
            if (!str.startsWith("{") || !str.endsWith("}")) {
                return null;
            } 

            String[] toParse = str.substring(1, str.length() - 1).split(",");
            for (String i : toParse) {
                String[] field = i.split(":");
                if (field.length != 2) {
                    return null;
                }

                String key = field[0].trim();
                String value = field[1].trim();
                if (key.startsWith("\"") && key.endsWith("\"")) {
                    key = key.substring(1, key.length()-1);
                }
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length()-1);
                }

                json.put(key, value);
            }
            return json;
        }

    }

    static class UserGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle POST request for /test
            if ("GET".equals(exchange.getRequestMethod())) {
                // try to parse id
                int id;
                String []tokens = exchange.getRequestURI().toString().split("/");
                try {
                    id = Integer.parseInt(tokens[2]);
                } catch (NumberFormatException e) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();
                    return;
                }

                // send json
                User user = db.get(id);
                if (user == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                sendResponse(exchange, userToJson(user));  
                exchange.close();
                return;
            } else {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
                sendResponse(exchange, "{}");
                exchange.close();
                return;
            }
        }
    }

    static class ShutdownHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                storeUsers(dbPath);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                sendResponse(exchange, "{}");
                exchange.close();
                server.stop(0);
                System.exit(0);               
            } else {
                // Send a 405 Method Not Allowed response for non-POST requests
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
                sendResponse(exchange, "{}");
                exchange.close();
                return;
            }
            
        }
    }

    static class WipeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                wipeDB(dbPath);
                db.clear();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                sendResponse(exchange, "{}");
                exchange.close();             
            } else {
                // Send a 405 Method Not Allowed response for non-POST requests
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
                sendResponse(exchange, "{}");
                exchange.close();
                return;
            }
            
        }
    }

    private static void sendResponse(HttpExchange exchange, String response) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes(StandardCharsets.UTF_8));
        os.close();
    }

    public static int getPort(String service, String configFileName) throws IOException {
        String config = new String(Files.readAllBytes(Paths.get(configFileName)));
        int start = config.indexOf(":", config.indexOf("\"port\"", config.indexOf(service))) + 1;
        int end = config.indexOf(",", start);
        int port = Integer.parseInt(config.substring(start, end).trim());
        return port;
    }

    public static String getIp(String service, String configFileName) throws IOException {
        String config = new String(Files.readAllBytes(Paths.get(configFileName)));
        int start = config.indexOf("\"", config.indexOf(":", config.indexOf("\"ip\"", config.indexOf(service)))) + 1;
        int end = config.indexOf("\"", start);
        String ip = config.substring(start, end).trim();
        return ip;
    }
    private static String userToJson(User user) {
        String json = "{" + "\"id\":" + user.id + "," + 
            "\"username\":" + "\"" + user.username + "\"" + "," +
            "\"email\":" + "\"" + user.email + "\"" + "," +
            "\"password\":" + "\"" + user.password.toUpperCase() + "\"" +
            "}";
        return json;
    }

    private static void loadUsers(String pathName) throws IOException, FileNotFoundException {
        File file = new File(pathName);
        file.createNewFile();
        
        // load users
        Scanner reader = new Scanner(file);
        while (reader.hasNextLine()) {
            String userString = reader.nextLine();
            if (userString.isEmpty()) {
                continue;
            }
            String userData[] = userString.trim().split("\\|");
            int user_id = Integer.parseInt(userData[0]);
            User user = new User(user_id, userData[1], userData[2], userData[3]);
            db.put(user_id, user);
        }
        reader.close();
    }

    private static void storeUsers(String pathName) throws IOException, FileNotFoundException {
        // store users
        FileWriter writer = new FileWriter(pathName);
        for (User user : db.values()) {
            StringBuilder userData = new StringBuilder();
            userData.append(user.id);
            userData.append("|");
            userData.append(user.username);
            userData.append("|");
            userData.append(user.email);
            userData.append("|");
            userData.append(user.password);
            userData.append("\n");
            writer.write(userData.toString());
        }
        writer.close();
    }

    private static void wipeDB(String pathName) throws IOException {
        new FileWriter(pathName, false).close();
    }

    private static boolean validEmail(String email) {
        return email.contains("@");
    }

}

