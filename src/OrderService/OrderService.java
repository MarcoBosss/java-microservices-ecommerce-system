import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrderService {
    // data base
    static Map<Integer, Order> db = new ConcurrentHashMap<>();
    static Map<Integer, Map<Integer, Integer>> purchases = new ConcurrentHashMap<>();
    static Integer orderId = 0;
    static String configFileName;
    static HttpServer server;
    static AtomicBoolean firstCommand;
    final static String dbPath = "src/db/order.db";

    public static void main(String[] args) throws IOException {
        configFileName = args[0];
        firstCommand = new AtomicBoolean(true);
        int port = getPort("OrderService", configFileName);
        loadOrders(dbPath);
        rebuildPurchases();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(20)); // Adjust the pool size as needed
        server.createContext("/order", new OrderPostHandler());
        server.createContext("/user", new UserPostHandler());
        server.createContext("/user/", new UserGetHandler());
        server.createContext("/user/purchased/", new UserPurchasedGetHandler());
        server.createContext("/product", new ProductPostHandler());
        server.createContext("/product/", new ProductGetHandler());
        server.createContext("/shutdown", new ShutdownHandler());
        server.createContext("/restart", new RestartHandler());
        server.start();
    }

    static class Order {
        Integer id, user_id, product_id, quantity;
        String status;

        Order(Integer id, Integer user_id, Integer product_id, Integer quantity) {
            this.id = id;
            this.user_id = user_id;
            this.product_id = product_id;
            this.quantity = quantity;
        }   
    }

    static class Result {
        int status;
        String body;

        Result(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }

    static class OrderPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = getRequestBody(exchange);
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                if (contentType == null || !contentType.equals("application/json")) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{\"status\":\"" + "Invalid Request" + "\"}");
                    exchange.close();
                    return;
                }
                Map<String, String> json = stringToJSON(body);
                if (json == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{\"status\":\"" + "Invalid Request" + "\"}");
                    exchange.close();   
                    return;                 
                }
                
                String command = json.get("command");
                Order orderData = jsonToOrder(json);
                if (orderData == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{\"status\":\"" + "Invalid Request" + "\"}");
                    exchange.close();
                    return;
                }
                int status;
                // command types
                if ("place order".equals(command)) {
                    status = placeOrder(orderData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    if (status == 200) {
                        sendResponse(exchange, orderToJson(db.get(orderData.id)));
                    } else {
                        sendResponse(exchange, "{\"status\":\"" + orderData.status + "\"}");
                    }
                } else {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, 0);
                    sendResponse(exchange, "{\"status\":\"" + "Invalid Request" + "\"}");
                }
            } else {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
                sendResponse(exchange, "{\"status\":\"" + "Invalid Request" + "\"}");
            }
            exchange.close();
            return;
        }
    }

    static class UserPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "UserService";
            String ip = getIp(service, configFileName);
            int port = getPort(service, configFileName);
            Result result = forwardRequest(ip, port, requestURI, requestMethod, body);
            exchange.sendResponseHeaders(result.status, 0);
            sendResponse(exchange, result.body);
            exchange.close();       
            return;
        }
    }

    static class UserGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "UserService";
            String ip = getIp(service, configFileName);
            int port = getPort(service, configFileName);
            Result result = forwardRequest(ip, port, requestURI, requestMethod, body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(result.status, 0);
            sendResponse(exchange, result.body);
            exchange.close();       
            return;
        }
    }

    static class UserPurchasedGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            // Handle POST request for /test
            if ("GET".equals(exchange.getRequestMethod())) {
                // try to parse user id
                int id;
                String []tokens = exchange.getRequestURI().toString().split("/");
                try {
                    id = Integer.parseInt(tokens[3]);
                } catch (NumberFormatException e) {
                    // bad route
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();
                    return;
                }

                // check if user id exist
                String service = "UserService";
                String ip = getIp(service, configFileName);
                int port = getPort(service, configFileName);
                int userStatus = forwardRequstStatus(ip, port, "/user/"+id, "GET");

                // user id does not exist
                if (userStatus != 200) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, -1);
                    exchange.close();  
                    return;
                }

                String response = getPurchaseHistory(id);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                sendResponse(exchange, response);  
                exchange.close();                    
                return;
            } else {
                // should be get request
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
                sendResponse(exchange, "{}");
                exchange.close();
                return;
            }
        }
    }

    static class ProductPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "ProductService";
            String ip = getIp(service, configFileName);
            int port = getPort(service, configFileName);
            Result result = forwardRequest(ip, port, requestURI, requestMethod, body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(result.status, 0);
            sendResponse(exchange, result.body);
            exchange.close();       
            return;
        }
    }

    static class ProductGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "ProductService";
            String ip = getIp(service, configFileName);
            int port = getPort(service, configFileName);
            Result result = forwardRequest(ip, port, requestURI, requestMethod, body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(result.status, 0);
            sendResponse(exchange, result.body);
            exchange.close();       
            return;
        }
    }

    static class ShutdownHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            if ("POST".equals(exchange.getRequestMethod())) {
                storeOrder(dbPath);
                // forward shutdown request to ISCS
                forwardRequest(
                    getIp("InterServiceCommunication", configFileName), 
                    getPort("InterServiceCommunication", configFileName),
                    "/shutdown", "POST", "");
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
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

    static class RestartHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // check if this is the first command
                firstCommand.compareAndSet(true, false);   
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                sendResponse(exchange, "{}");
                exchange.close();    
                return;
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

    @SuppressWarnings("deprecation")
    private static Result forwardRequest(String ip, int port, String requestURI, String requestMethod, String body) throws IOException {
        // set up connection
        String urlString = "http://" + ip + ":" + port + requestURI;
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(requestMethod);

        // forward body if POST
        if ("POST".equals(requestMethod)) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            OutputStream os = connection.getOutputStream();
            os.write(body.getBytes(StandardCharsets.UTF_8));
            os.close();
        }

        // get result
        Result result;
        int status = connection.getResponseCode();
        InputStream is;
        byte[] response;
        if (status >= 200 && status < 400) {
            is = connection.getInputStream();
        } else {
            is = connection.getErrorStream();
        }
        if (is == null) {
            response = new byte[0];
        } else {
            response = is.readAllBytes();
        }
        connection.disconnect();
        result = new Result(status, new String(response, StandardCharsets.UTF_8));

        return result;
    }

    @SuppressWarnings("deprecation")
    private static Integer forwardRequstStatus(String ip, int port, String requestURI, String requestMethod) throws IOException {
        // set up connection
        String urlString = "http://" + ip + ":" + port + requestURI;
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(requestMethod);

        // return status code
        int status = connection.getResponseCode();
        connection.disconnect();
        return status;
    }

    @SuppressWarnings("deprecation")
    private static String getBody(String ip, int port, String requestURI, String requestMethod) throws IOException {
        // set up connection
        String urlString = "http://" + ip + ":" + port + requestURI;
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(requestMethod);

        // read response
        int status = connection.getResponseCode();
        InputStream is;
        byte[] response;
        if (status == 200) {
            is = connection.getInputStream();
        } else {
            is = connection.getErrorStream();
        }
        if (is == null) {
            response = new byte[0];
        } else {
            response = is.readAllBytes();
        }
        connection.disconnect();

        // get quantity
        String responseString = new String(response, StandardCharsets.UTF_8);
        return responseString;        
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

    private static Order jsonToOrder(Map<String, String> json) {
        int id = orderId;
        try {
            Integer user_id = Integer.parseInt(json.get("user_id"));
            Integer product_id = Integer.parseInt(json.get("product_id"));
            Integer quantity = Integer.parseInt(json.get("quantity"));
            Order order = new Order(id, user_id, product_id, quantity);
            orderId++;
            return order;
        } catch (NumberFormatException e) {
            return null;
        }
    }


    private static Integer placeOrder(Order orderData) throws IOException {
        if (orderData.user_id == null || orderData.product_id == null || orderData.quantity == null) {
            orderData.status = "Invalid Request";
            return 404;
        }

        if (orderData.quantity < 0) {
            orderData.status = "Invalid Request";
            return 400;            
        }

        // check user exists
        String service = "UserService";
        String ip = getIp(service, configFileName);
        int port = getPort(service, configFileName);
        int userStatus = forwardRequstStatus(ip, port, "/user/"+orderData.user_id, "GET");
        if (userStatus != 200) {
            orderData.status = "Invalid Request";
            return userStatus;
        }

        // check product exists
        service = "ProductService";
        ip = getIp(service, configFileName);
        port = getPort(service, configFileName);
        int productStatus = forwardRequstStatus(ip, port, "/product/"+orderData.product_id, "GET");
        if (productStatus != 200) {
            orderData.status = "Invalid Request";
            return productStatus;
        }

        // get product quantity
        String body = getBody(ip, port, "/product/"+orderData.product_id, "GET");
        int start = body.indexOf(":", body.indexOf("\"quantity\"")) + 1;
        int end = body.indexOf("}", start);
        int quantityInStock = Integer.parseInt(body.substring(start, end).trim());

        // check if product is in stock
        if (quantityInStock < orderData.quantity) {
            orderData.status = "Exceeded quantity limit";
            return 400;                    
        }

        // update quantity
        String json = "{\"command\":\"update\","
                    + "\"id\"" + ":" + orderData.product_id + "," 
                    + "\"quantity\"" + ":" + (quantityInStock - orderData.quantity)
                    + "}";
        int status = forwardRequest(ip, port, "/product", "POST", json).status;
        if (status != 200) {
            orderData.status = "Invalid Request";
            return status;
        } 
        orderData.status = "Success";
        db.put(orderData.id, orderData);
        recordPurchase(orderData.user_id, orderData.product_id, orderData.quantity);
        return status;
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

    private static String orderToJson(Order order) {
        String json = "{" + "\"product_id\":" + order.product_id + "," + 
                            "\"user_id\":" + order.user_id + "," + 
                            "\"quantity\":" + order.quantity + "," + 
                            "\"status\":" + "\"" + order.status + "\"" +
                            "}";
        return json;
    }

    private static void recordPurchase(int user_id, int product_id, int quantity) {
        Map<Integer, Integer> purchaseHistory;
        if (!purchases.containsKey(user_id)) {
            purchaseHistory = new ConcurrentHashMap<>();
            purchases.put(user_id, purchaseHistory);
        } else {
            purchaseHistory = purchases.get(user_id);
        }

        if (!purchaseHistory.containsKey(product_id)) {
            purchaseHistory.put(product_id, quantity);
        } else {
            purchaseHistory.merge(product_id, quantity, Integer::sum);
        }
    }

    private static String getPurchaseHistory(int user_id) {
        // user id did not purchase anything
        if (!purchases.containsKey(user_id)) {
            return "{}";
        }

        Map<Integer, Integer> purchaseHistory = purchases.get(user_id);
        StringBuilder response = new StringBuilder();
        response.append("{");
        
        boolean first = true;
        for (int product_id : purchaseHistory.keySet()) {
            if (!first) {
                response.append(", ");
            }
            first = false;
            response.append("\"");
            response.append(product_id);
            response.append("\":");
            response.append(purchaseHistory.get(product_id));
        }

        response.append("}");
        return response.toString();
    }

    private static void rebuildPurchases() {
        for (Order order : db.values()) {
            recordPurchase(order.user_id, order.product_id, order.quantity);
        }
    }

    private static void firstCommandAction() throws IOException {
        if (!firstCommand.compareAndSet(true, false)) {
            return;
        }
        // wipe all db
        db.clear();
        wipeDB(dbPath);
        purchases.clear();
        orderId = 0;
        forwardRequest(
            getIp("InterServiceCommunication", configFileName), 
            getPort("InterServiceCommunication", configFileName),
            "/wipe", "POST", "");
    }

    private static void loadOrders(String pathName) throws IOException, FileNotFoundException {
        File file = new File(pathName);
        file.createNewFile();
        Scanner reader = new Scanner(file);

        // load order id
        if (reader.hasNextLine()) {
            orderId = Integer.parseInt(reader.nextLine());
        }

        // load orders
        while (reader.hasNextLine()) {
            String orderString = reader.nextLine();
            if (orderString.isEmpty()) {
                continue;
            }
            String orderData[] = orderString.trim().split("\\|");
            int order_id = Integer.parseInt(orderData[0]);
            Order order = new Order(
                order_id, 
                Integer.parseInt(orderData[1]), 
                Integer.parseInt(orderData[2]), 
                Integer.parseInt(orderData[3]));
            db.put(order_id, order);
        }
        reader.close();
    }

    private static void storeOrder(String pathName) throws IOException, FileNotFoundException {
        // store orders
        FileWriter writer = new FileWriter(pathName);
        writer.write(orderId + "\n");
        for (Order order : db.values()) {
            StringBuilder orderData = new StringBuilder();
            orderData.append(order.id);
            orderData.append("|");
            orderData.append(order.user_id);
            orderData.append("|");
            orderData.append(order.product_id);
            orderData.append("|");
            orderData.append(order.quantity);
            orderData.append("\n");
            writer.write(orderData.toString());
        }
        writer.close();
    }

    private static void wipeDB(String pathName) throws IOException {
        new FileWriter(pathName, false).close();
    }
}