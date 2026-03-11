import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import java.sql.Connection;
import java.sql.ResultSet;

import common.HttpResponse;
import common.ConfigUtils;
import common.DbUtils;
import common.HttpUtils;

public class OrderService {
    // data base
    static Integer orderId = 0;
    static String configFileName;
    static HttpServer server;
    static AtomicBoolean firstCommand;
    static String dbIp;
    static int dbPort;
    static String dbName = "ecommerce";
    static String dbPassword = "password";

    final static String dbPath = "src/db/order.db";

    public static void main(String[] args) throws IOException {
        firstCommand = new AtomicBoolean(true);
        configFileName = ConfigUtils.loadConfigFile(args[0]);
        dbIp = ConfigUtils.getIp("Database", configFileName);
        dbPort = ConfigUtils.getPort("Database", configFileName);

        int port = ConfigUtils.getPort("OrderService", configFileName);
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(512)); // Adjust the pool size as needed
        server.createContext("/order", new OrderPostHandler());
        server.createContext("/user", new UserPostHandler());
        server.createContext("/user/", new UserGetHandler());
        server.createContext("/user/purchased/", new UserPurchasedGetHandler());
        server.createContext("/product", new ProductPostHandler());
        server.createContext("/product/", new ProductGetHandler());
        // server.createContext("/shutdown", new ShutdownHandler());
        // server.createContext("/restart", new RestartHandler());
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

    static class OrderPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{\\\"status\\\":\\\"\" + \"Invalid Request\" + \"\\\"}");
                return;
            }

            // check if content type is json
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType == null || !contentType.equals("application/json")) {
                HttpUtils.sendJsonResponse(exchange, 400, "{\"status\":\"" + "Invalid Request" + "\"}");
                return;
            }

            // check if body is valid json
            String body = HttpUtils.getRequestBody(exchange);
            Map<String, String> json = HttpUtils.stringToJSON(body);
            if (json == null) {
                HttpUtils.sendJsonResponse(exchange, 400, "{\"status\":\"" + "Invalid Request" + "\"}");
                return;                 
            }
            
            String command = json.get("command");
            Order orderData = jsonToOrder(json);
            if (orderData == null) {
                HttpUtils.sendJsonResponse(exchange, 400, "{\"status\":\"" + "Invalid Request" + "\"}");
                return;
            }

            // process user command and send response
            HttpResponse response = orderCommand(command, orderData);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);    
        }
    }

    static class UserPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = HttpUtils.getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "UserService";
            String ip = ConfigUtils.getIp(service, configFileName);
            int port = ConfigUtils.getPort(service, configFileName);
            HttpResponse response = HttpUtils.forwardRequest(ip, port, requestURI, requestMethod, body);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);   
            return;
        }
    }

    static class UserGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = HttpUtils.getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "UserService";
            String ip = ConfigUtils.getIp(service, configFileName);
            int port = ConfigUtils.getPort(service, configFileName);
            HttpResponse response = HttpUtils.forwardRequest(ip, port, requestURI, requestMethod, body);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);     
            return;
        }
    }

    static class UserPurchasedGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            // non-GET requests
            if (!"GET".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // try to parse user id
            int id;
            String []tokens = exchange.getRequestURI().toString().split("/");
            try {
                id = Integer.parseInt(tokens[3]);
            } catch (NumberFormatException e) {
                // bad route
                HttpUtils.sendJsonResponse(exchange, 400, "{}");  
                return;
            }

            // check if user id exist
            String service = "UserService";
            String ip = ConfigUtils.getIp(service, configFileName);
            int port = ConfigUtils.getPort(service, configFileName);
            int userStatus = HttpUtils.forwardRequstStatus(ip, port, "/user/"+id, "GET");

            // user id does not exist
            if (userStatus != 200) {
                HttpUtils.sendJsonResponse(exchange, 404, ""); 
                return;
            }

            String response = getPurchaseHistory(id);
            HttpUtils.sendJsonResponse(exchange, 200, response);                
            return;
        }
    }

    static class ProductPostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = HttpUtils.getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "ProductService";
            String ip = ConfigUtils.getIp(service, configFileName);
            int port = ConfigUtils.getPort(service, configFileName);
            HttpResponse response = HttpUtils.forwardRequest(ip, port, requestURI, requestMethod, body);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);     
            return;
        }
    }

    static class ProductGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            firstCommandAction();
            String body = HttpUtils.getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();
            String service = "ProductService";
            String ip = ConfigUtils.getIp(service, configFileName);
            int port = ConfigUtils.getPort(service, configFileName);
            HttpResponse response = HttpUtils.forwardRequest(ip, port, requestURI, requestMethod, body);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);            
            return;
        }
    }


    private static HttpResponse orderCommand(String command, Order orderData) throws IOException {
        if ("place order".equals(command)) {
            return placeOrder(orderData);
        }
        // service not found
        return new HttpResponse(404, "{\"status\":\"" + "Invalid Request" + "\"}");
        }

    private static Order jsonToOrder(Map<String, String> json) {
        try {
            Integer user_id = Integer.parseInt(json.get("user_id"));
            Integer product_id = Integer.parseInt(json.get("product_id"));
            Integer quantity = Integer.parseInt(json.get("quantity"));
            Order order = new Order(null, user_id, product_id, quantity);
            orderId++;
            return order;
        } catch (NumberFormatException e) {
            return null;
        }
    }


    private static HttpResponse placeOrder(Order orderData) throws IOException {
        if (orderData.user_id == null || orderData.product_id == null || orderData.quantity == null) {
            return new HttpResponse(404, "{\"status\":\"Invalid Request\"}");
        }

        if (orderData.quantity < 0) {
            return new HttpResponse(400, "{\"status\":\"Invalid Request\"}");   
        }

        // check user exists
        String service = "UserService";
        String ip = ConfigUtils.getIp(service, configFileName);
        int port = ConfigUtils.getPort(service, configFileName);
        int userStatus = HttpUtils.forwardRequstStatus(ip, port, "/user/"+orderData.user_id, "GET");
        if (userStatus != 200) {
            return new HttpResponse(userStatus, "{\"status\":\"Invalid Request\"}");
        }

        // check product exists
        service = "ProductService";
        ip = ConfigUtils.getIp(service, configFileName);
        port = ConfigUtils.getPort(service, configFileName);
        int productStatus = HttpUtils.forwardRequstStatus(ip, port, "/product/"+orderData.product_id, "GET");
        if (productStatus != 200) {
            return new HttpResponse(productStatus, "{\"status\":\"Invalid Request\"}");
        }

        // get product quantity
        String body = HttpUtils.getBody(ip, port, "/product/"+orderData.product_id, "GET");
        int start = body.indexOf(":", body.indexOf("\"quantity\"")) + 1;
        int end = body.indexOf("}", start);
        int quantityInStock = Integer.parseInt(body.substring(start, end).trim());

        // check if product is in stock
        if (quantityInStock < orderData.quantity) {
            return new HttpResponse(400, "{\"status\":\"Exceeded quantity limit\"}");                 
        }

        // update quantity
        String json = "{\"command\":\"update\","
                    + "\"id\"" + ":" + orderData.product_id + "," 
                    + "\"quantity\"" + ":" + (quantityInStock - orderData.quantity)
                    + "}";
        int status = HttpUtils.forwardRequest(ip, port, "/product", "POST", json).status;
        if (status != 200) {
            return new HttpResponse(status, "{\"status\":\"Invalid Request\"}");
        } 
        Integer id;
        try {
            id = insertOrder(orderData);
        } catch (Exception e) {
            return new HttpResponse(500, "{\"status\":\"Invalid Request\"}");
        }
        if (id == null) {
            return new HttpResponse(500, "{\"status\":\"Invalid Request\"}");
        }
        orderData.id = id;
        orderData.status = "Success";
        return new HttpResponse(status, orderToJson(orderData));
    }

    private static String orderToJson(Order order) {
        String json = "{" + "\"id\":" + order.id + "," +
                            "\"product_id\":" + order.product_id + "," + 
                            "\"user_id\":" + order.user_id + "," + 
                            "\"quantity\":" + order.quantity + "," + 
                            "\"status\":" + "\"" + order.status + "\"" +
                            "}";
        return json;
    }

    private static String getPurchaseHistory(int id) {
        String sql = """
            SELECT product_id, SUM(quantity) AS total
            FROM orders
            WHERE user_id = ?
            GROUP BY product_id
            """;
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            statement.setInt(1, id);

            try (ResultSet result = statement.executeQuery()) {
                StringBuilder json = new StringBuilder(); 
                json.append("{");

                boolean first = true;

                while (result.next()) {
                    if (!first) json.append(",");
                    first = false;

                    int product_id = result.getInt("product_id");
                    int quantity = result.getInt("total");

                    json.append("\"").append(product_id).append("\":").append(quantity);
                }
                json.append("}");
                return json.toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "{}";
    }

    private static void firstCommandAction() throws IOException {
        if (!firstCommand.compareAndSet(true, false)) {
            return;
        }
        // wipe all db
        // wipeDB(dbPath);
        orderId = 0;
        HttpUtils.forwardRequest(
            ConfigUtils.getIp("InterServiceCommunication", configFileName), 
            ConfigUtils.getPort("InterServiceCommunication", configFileName),
            "/wipe", "POST", "");
    }

    private static Integer insertOrder(Order order) throws Exception{
        String sql = "INSERT INTO orders (product_id, user_id, quantity) VALUES (?, ?, ?) RETURNING id";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            
            statement.setInt(1, order.product_id);
            statement.setInt(2, order.user_id);
            statement.setInt(3, order.quantity);

            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return result.getInt("id");
                }
            }
        } 
        return null;
    }
}