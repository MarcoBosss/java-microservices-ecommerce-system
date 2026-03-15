import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.io.FileNotFoundException;

import common.HttpResponse;
import common.ConfigUtils;
import common.DbUtils;
import common.HttpUtils;
import common.JsonUtils;

public class OrderService {
    // service info
    final static int port = 14003;
    final static int flushTime = 2;
    static HttpServer server;
    static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // database info
    final static int poolSize = 4;
    static BlockingQueue<Connection> dbPool = new ArrayBlockingQueue<>(poolSize);
    static JsonUtils.Service dbService;
    static String dbName = "orderdb";
    static String dbPassword = "password";

    // cache
    static Map<Integer, Order> dirtyOrders = new ConcurrentHashMap<>();
    static Map<Integer, Map<Integer, Integer>> purchases = new ConcurrentHashMap<>();
    static Map<Integer, Object> userLocks = new ConcurrentHashMap<>();

    // other services info
    static List<JsonUtils.Service> userServices = new ArrayList<>();
    static List<JsonUtils.Service> productServices = new ArrayList<>();
    
    // restart info
    static AtomicBoolean firstCommand;
    static String configFileName;

    public static void main(String[] args) throws Exception {
        // load services info
        configFileName = args[0];
        loadServices(configFileName);
        JsonUtils.Config config = new Gson().fromJson(
            new FileReader(configFileName), 
            JsonUtils.Config.class
        );
        JsonUtils.ServiceConfig dbConfig = config.OrderDatabase;
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

        // restart info init
        firstCommand = new AtomicBoolean(true);

        // service init
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(256));
        server.createContext("/order", new OrderPostHandler());
        server.createContext("/user/purchased/", new UserPurchasedGetHandler());
        server.createContext("/livecheck", new LiveCheckHandler());
        // server.createContext("/shutdown", new ShutdownHandler());
        // server.createContext("/restart", new RestartHandler());
        server.createContext("/standby", new StandbyHandler());
        server.createContext("/standby/user", new StandbyUserHandler());
        server.createContext("/standby/product", new StandbyProductHandler());
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
            if (checkUserExistence(id)) {
                String response = getPurchaseHistory(id);
                HttpUtils.sendJsonResponse(exchange, 200, response);                
                return;
            }
            HttpUtils.sendJsonResponse(exchange, 404, "{}"); 
            return;
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
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
        }
    }

    static class StandbyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }
            // flush to db 
            try {
                flushToDB();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // clear cache
            dirtyOrders.clear();
            purchases.clear();
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
        }
    }

    static class StandbyUserHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // parse id
            Integer index;
            String []tokens = exchange.getRequestURI().toString().split("/");
            try {
                index = Integer.parseInt(tokens[tokens.length-1]);
            } catch (NumberFormatException e) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // repace service
            replaceService(index, userServices);
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
        }
    }

    static class StandbyProductHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // parse id
            Integer index;
            String []tokens = exchange.getRequestURI().toString().split("/");
            try {
                index = Integer.parseInt(tokens[tokens.length-1]);
            } catch (NumberFormatException e) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // repace service
            replaceService(index, productServices);
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
        }
    }

    private static void replaceService(int index, List<JsonUtils.Service> services) throws IOException {
        JsonUtils.Service failed = services.get(index);
        JsonUtils.Service backup = services.get(services.size()-1);
        services.set(index, backup);
        services.set(services.size() - 1, failed);
    }

    private static boolean checkUserExistence(int id) throws IOException{
        JsonUtils.Service service = selectService(id, userServices);
        HttpResponse response = HttpUtils.forwardRequest(
            service.ip, 
            service.port, 
            "/user/" + id, 
            "GET", 
            ""
        );
        return (response.status == 200);
    }

    private static HttpResponse tryUpdateProduct(int id, int quantity) throws IOException{
        JsonUtils.Service service = selectService(id, productServices);
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":").append(id).append(",");
        json.append("\"quantity\":").append(quantity);
        json.append("}");
        HttpResponse response = HttpUtils.forwardRequest(
            service.ip, 
            service.port, 
            "/reserve", 
            "POST", 
            json.toString()
        );
        return response;
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
            Integer id = Integer.parseInt(json.get("id"));
            return new Order(id, user_id, product_id, quantity);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static HttpResponse placeOrder(Order orderData) throws IOException {
        if (orderData.user_id == null || orderData.product_id == null || orderData.quantity == null) {
            return new HttpResponse(404, "{\"status\":\"Invalid Request\"}");
        }

        if (orderData.quantity <= 0) {
            return new HttpResponse(400, "{\"status\":\"Invalid Request\"}");   
        }

        synchronized (getLock(orderData.user_id)) {
            // check user exists
            if (!checkUserExistence(orderData.user_id)) {
                return new HttpResponse(404, "{\"status\":\"Invalid Request\"}");
            }

            // check product exists and quantity
            HttpResponse response = tryUpdateProduct(orderData.product_id, orderData.quantity);
            if (response.status == 404) {
                return new HttpResponse(404, "{\"status\":\"Invalid Request\"}");
            } else if (response.status == 200) {
                orderData.status = "Success";
                dirtyOrders.put(orderData.id, orderData);
                return new HttpResponse(200, orderToJson(orderData));
            } else if (response.status == 400) {
                return new HttpResponse(400, response.body); 
            } else {
                return new HttpResponse(500, "{}");
            }
        }
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

    private static String purchaseHistoryToString(Map<Integer, Integer> purchaseHistory) {
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

    private static Map<Integer, Integer> resultToPurchaseHistory(ResultSet result) throws SQLException {
        Map<Integer, Integer> purchaseHistory = new HashMap<>();
        while (result.next()) {
            purchaseHistory.put(result.getInt("product_id"), result.getInt("total"));
        }
        return purchaseHistory;
    }

    private static Map<Integer, Integer> recordRecentPurchase(int user_id, Map<Integer, Integer> purchaseHistory) {
        Map<Integer, Integer> copy = new HashMap<>(purchaseHistory);
        for (Order order : dirtyOrders.values()) {
            if (order.user_id == user_id) {
                copy.merge(order.product_id, order.quantity, Integer::sum);
            }
        }
        return copy;
    }

    private static String getPurchaseHistory(int user_id) {
        synchronized (getLock(user_id)) {
            // get puchase history from cache
            Map<Integer, Integer> purchaseHistory = purchases.get(user_id);
            if (purchaseHistory != null) {
                return purchaseHistoryToString(recordRecentPurchase(user_id, purchaseHistory));
            } 

            // get purchase history from memory
            String sql = """
                SELECT product_id, SUM(quantity) AS total
                FROM orders
                WHERE user_id = ?
                GROUP BY product_id
                """;
            Connection dbConnection = null;
            try {
                dbConnection = dbPool.take();
                try (var statement = dbConnection.prepareStatement(sql)) {
                    statement.setInt(1, user_id);
                    try (ResultSet result = statement.executeQuery()) {
                        purchaseHistory = resultToPurchaseHistory(result);
                        purchases.put(user_id, purchaseHistory);
                        return purchaseHistoryToString(recordRecentPurchase(user_id, purchaseHistory));
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
            return "{}";  
        }
    }

    private static void firstCommandAction() throws IOException {
        if (!firstCommand.compareAndSet(true, false)) {
            return;
        }
        // wipe all db
        // wipeDB(dbPath);
        // orderId = 0;
        HttpUtils.forwardRequest(
            ConfigUtils.getIp("InterServiceCommunication", configFileName), 
            ConfigUtils.getPort("InterServiceCommunication", configFileName),
            "/wipe", "POST", "");
    }

    private static void insertOrdersToDB(List<Order> orders){
        if (orders.isEmpty()) return;
        String sql = """
            INSERT INTO orders (id, product_id, user_id, quantity) 
            VALUES (?, ?, ?, ?) 
            ON CONFLICT (id)
            DO UPDATE SET
                product_id = EXCLUDED.product_id, 
                user_id = EXCLUDED.user_id, 
                quantity = EXCLUDED.quantity
            """;
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            dbConnection.setAutoCommit(false);
            try (var statement = dbConnection.prepareStatement(sql)) {
                for (Order order:orders) {
                    statement.setInt(1, order.id);
                    statement.setInt(2, order.product_id);
                    statement.setInt(3, order.user_id);
                    statement.setInt(4, order.quantity);
                    statement.addBatch();
                }
                statement.executeBatch();
                dbConnection.commit();
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
        List<Order> ordersToFlush = new ArrayList<>();

        for (Integer id : Set.copyOf(dirtyOrders.keySet())) {
            Order order = dirtyOrders.get(id);
            if (order == null) continue;
            synchronized(getLock(order.user_id)) {
                order = dirtyOrders.remove(id);
                if (order != null) {
                    recordPurchase(order.user_id, order.product_id, order.quantity);
                    ordersToFlush.add(order);
                }
            }
        }

        insertOrdersToDB(ordersToFlush);
    }

    private static void loadServices(String configFileName) throws FileNotFoundException {
        JsonUtils.Config config = new Gson().fromJson(new FileReader(configFileName), JsonUtils.Config.class);
        for (JsonUtils.ServiceConfig service : config.UserServices) {
            userServices.add(new JsonUtils.Service("UserService", service.ip, service.port));
        }

        for (JsonUtils.ServiceConfig service : config.ProductServices) {
            productServices.add(new JsonUtils.Service("ProductService", service.ip, service.port));
        }
    }

    private static JsonUtils.Service selectService(int id, List<JsonUtils.Service> services) {
        int index = Math.floorMod(id * 0x9E3779B9, services.size()-1);
        return services.get(index);
    }

    private static void initDbPool() throws SQLException{
        for (int i=0;i<poolSize;i++) {
            Connection dbConnection = DbUtils.getDBConnection(
                dbService.ip, dbService.port, dbName, dbPassword
            );
            dbPool.add(dbConnection);
        }
    }     
    
    private static void recordPurchase(int user_id, int product_id, int quantity) {
        Map<Integer, Integer> purchaseHistory = purchases.get(user_id);
        if (purchaseHistory == null) {
            purchaseHistory = new HashMap<>();
            purchases.put(user_id, purchaseHistory);
        }
        purchaseHistory.merge(product_id, quantity, Integer::sum);
    }
    
    private static Object getLock(int user_id) {
        return userLocks.computeIfAbsent(user_id, k -> new Object());
    }
}