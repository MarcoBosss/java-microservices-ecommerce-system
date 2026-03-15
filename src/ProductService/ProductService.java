import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import common.HttpResponse;
import common.DbUtils;
import common.HttpUtils;
import common.JsonUtils;

public class ProductService {
    // service info
    final static int port = 14002;
    final static int flushTime = 2;
    static HttpServer server;
    static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // database info
    final static int poolSize = 4;
    static BlockingQueue<Connection> dbPool = new ArrayBlockingQueue<>(poolSize); 
    static JsonUtils.Service dbService;
    static String dbName = "productdb";
    static String dbPassword = "password";

    // cache
    static Map<Integer, Product> cache = new ConcurrentHashMap<>();
    static Map<Integer, Product> dirtyProducts = new ConcurrentHashMap<>();
    static Set<Integer> deletedProducts = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) throws Exception {
        // database init
        JsonUtils.Config config = new Gson().fromJson(
            new FileReader(args[0]), 
            JsonUtils.Config.class
        );
        JsonUtils.ServiceConfig dbConfig = config.ProductDatabase;
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
        server.createContext("/product", new ProductPostHandler());
        server.createContext("/product/", new ProductGetHandler());
        server.createContext("/livecheck", new LiveCheckHandler());
        server.createContext("/shutdown", new ShutdownHandler());
        server.createContext("/wipe", new WipeHandler());
        server.createContext("/reserve", new ReservePostHandler());
        server.createContext("/standby", new StandbyHandler());
        server.start();
    }

    static class Product {
        Integer id, quantity;
        Float price;
        String productname, description;

        Product(Integer id, String productname, String description, Float price, Integer quantity) {
            this.id = id;
            this.productname = productname;
            this.description = description;
            this.price = price;
            this.quantity = quantity;
        }   
    }

    static class ProductPostHandler implements HttpHandler {
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

            // parse command and product data
            String command = json.get("command");
            Product productData = jsonToProduct(json);
            if (productData == null) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // process product command and send response
            HttpResponse response = productCommand(command, productData);
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);
        }

    }

    static class ProductGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-GET requests
            if (!"GET".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // parse id
            Integer id;
            String []tokens = exchange.getRequestURI().toString().split("/");
            try {
                id = Integer.parseInt(tokens[2]);
            } catch (NumberFormatException e) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // send response json
            Product product = getProduct(id);
            if (product == null) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }
            HttpUtils.sendJsonResponse(exchange, 200, productToJson(product));            
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

    static class ShutdownHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // storeProducts(dbPath);
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
            server.stop(0);
            System.exit(0);                           
        }
    }

    static class WipeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
            }
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

    static class ReservePostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // non-POST requests
            if (!"POST".equals(exchange.getRequestMethod())) {
                HttpUtils.sendJsonResponse(exchange, 405, "{}");
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

            // parse id and quanitty 
            Integer id = null;
            Integer quantity = null;
            if (json.containsKey("id") && json.containsKey("quantity")) {
                try {
                    id = Integer.parseInt(json.get("id"));
                    quantity = Integer.parseInt(json.get("quantity"));
                } catch (NumberFormatException e) {
                    HttpUtils.sendJsonResponse(exchange, 400, "{}");
                    return;
                }
            } 
            if (id == null || quantity == null || quantity <= 0) {
                HttpUtils.sendJsonResponse(exchange, 400, "{}");
                return;
            }

            // check product existence
            Product product = getProduct(id);
            if (product == null) {
                HttpUtils.sendJsonResponse(exchange, 404, "{}");
                return;
            }

            // update quantity
            synchronized (product) {
                if (product.quantity < quantity) {
                    HttpUtils.sendJsonResponse(exchange, 400, "{\"status\":\"Exceeded quantity limit\"}");
                    return;
                }
                product.quantity -= quantity;
                dirtyProducts.put(product.id, product);
                HttpUtils.sendJsonResponse(exchange, 200, "{}");
            }
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
            cache = new ConcurrentHashMap<>();
            dirtyProducts = new ConcurrentHashMap<>();
            deletedProducts = ConcurrentHashMap.newKeySet();
            HttpUtils.sendJsonResponse(exchange, 200, "{}");
        }
    }

    private static Product getProduct(int id) {
        // get product from cache
        Product product = dirtyProducts.get(id);
        if (product != null) return product;
        product = cache.get(id);
        if (product != null) return product;

        // get product from db
        String sql = "SELECT id, name, description, price, quantity FROM products WHERE id = ?";
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            try (var statement = dbConnection.prepareStatement(sql)) {
                statement.setInt(1, id);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        product = buildProduct(result);
                        cache.put(id, product);
                        return product;
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

    private static Product buildProduct(ResultSet result) throws SQLException {
        return new Product(
            result.getInt("id"), 
            result.getString("name"), 
            result.getString("description"), 
            result.getFloat("price"), 
            result.getInt("quantity"));
    }

    private static HttpResponse productCommand(String command, Product productData) {
        if ("create".equals(command)) {
            return createProduct(productData);
        } else if ("update".equals(command)) {
            return updateProduct(productData);
        } else if ("delete".equals(command)) {
            return deleteProduct(productData);
        }
        return new HttpResponse(404, "{}");
    }


    private static Product jsonToProduct(Map<String, String> json) {
        // parse id
        Integer id;
        String idString = json.get("id");
        if (idString == null) {
            return null;
        } else {
            try {
                id = Integer.parseInt(idString);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        // parse quantity
        Integer quantity;
        String quantityString = json.get("quantity");
        if (quantityString == null) {
            quantity = (Integer) null;
        } else {
            try {
                quantity = Integer.parseInt(quantityString);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        // parse price
        Float price;
        String priceString = json.get("price");
        if (priceString == null) {
            price = (Float) null;
        } else {
            try {
                price = Float.parseFloat(priceString);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        // parse name and description
        String productname = json.get("name");
        String description = json.get("description");

        Product product = new Product(id, productname, description, price, quantity);
        return product;
    }

    private static HttpResponse createProduct(Product productData) {
        if (getProduct(productData.id) != null) {
            return new HttpResponse(409, "{}");
        }
        if (productData.productname == null || productData.price == null || productData.quantity == null || productData.description == null) {
            return new HttpResponse(400, "{}");
        }
        if (productData.productname.equals("") || productData.description.equals("")) {
            return new HttpResponse(400, "{}");
        }
        if (productData.quantity < 0 || productData.price < 0) {
            return new HttpResponse(400, "{}");
        }
        deletedProducts.remove(productData.id);
        cache.put(productData.id, productData);
        dirtyProducts.put(productData.id, productData);
        return new HttpResponse(200, productToJson(productData));
    }

    private static HttpResponse updateProduct(Product productData) {
        Product product = getProduct(productData.id);
        // check if product exists
        if (product == null) {
            return new HttpResponse(404, "{}");
        }

        // update product data
        if (productData.productname != null) {
            if (productData.productname.equals("")) {
                return new HttpResponse(400, "{}");
            }
            product.productname = productData.productname;
        }
            if (productData.description != null) {
            if (productData.description.equals("")) {
                return new HttpResponse(400, "{}");
            }
            product.description = productData.description;
        }           
        if (productData.price != null) {
            if (productData.price < 0) {
                return new HttpResponse(400, "{}");
            }
            product.price = productData.price;
        }
        if (productData.quantity != null) {
            if (productData.quantity < 0) {
                return new HttpResponse(400, "{}");
            }
            product.quantity = productData.quantity;
        }
        dirtyProducts.put(productData.id, product);
        return new HttpResponse(200, productToJson(product));
    }

    private static HttpResponse deleteProduct(Product productData) {
        // check if product exists
        Product product = getProduct(productData.id);
        if (product == null) {
            return new HttpResponse(404, "{}");
        }

        // check if all fields exist
        if (productData.productname == null || productData.price == null || productData.quantity == null) {
            return new HttpResponse(400, "{}");
        }

        // delete if fields correspond
        if (productData.productname.equals(product.productname) && productData.price.equals(product.price) && productData.quantity.equals(product.quantity)) {
            cache.remove(productData.id);
            dirtyProducts.remove(productData.id);
            deletedProducts.add(productData.id);
            return new HttpResponse(200, "{}");
        }
        return new HttpResponse(401, "{}");
    }

    private static void insertProductsToDB(List<Product> products) {
        if (products.isEmpty()) return;
        String sql = """
            INSERT INTO products (id, name, description, price, quantity) 
            VALUES (?, ?, ?, ?, ?) 
            ON CONFLICT (id)
            DO UPDATE SET
                name = EXCLUDED.name, 
                description = EXCLUDED.description, 
                price = EXCLUDED.price, 
                quantity = EXCLUDED.quantity
            """;
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            dbConnection.setAutoCommit(false);
            try(var statement = dbConnection.prepareStatement(sql)) {
                for (Product product:products) {
                    statement.setInt(1, product.id);
                    statement.setString(2, product.productname);
                    statement.setString(3, product.description);
                    statement.setDouble(4, product.price);
                    statement.setInt(5, product.quantity);
                    statement.addBatch();
                }
                statement.executeBatch();
                dbConnection.commit();                
        } catch (Exception e) {
            dbConnection.rollback();
            throw e;
        } finally {
            dbConnection.setAutoCommit(true);
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

    private static String productToJson(Product product) {
        String json = "{" + "\"id\":" + product.id + "," + 
            "\"name\":" + "\"" + product.productname + "\"" + "," +
            "\"description\":" + "\"" + product.description + "\"" + "," +
            "\"price\":" + String.format(Locale.US, "%.2f", product.price) + "," +
            "\"quantity\":" + product.quantity +
            "}";
        return json;
    }

    private static void deleteProductsFromDB(List<Integer> ids) {
        if (ids.isEmpty()) return;
        String sql = "DELETE FROM products WHERE id = ?";
        Connection dbConnection = null;
        try {
            dbConnection = dbPool.take();
            dbConnection.setAutoCommit(false);
            try (var statement = dbConnection.prepareStatement(sql)) {
                for (Integer id:ids) {
                    statement.setInt(1, id);
                    statement.addBatch();
                }
                statement.executeBatch();
                dbConnection.commit();
        } catch (Exception e) {
            dbConnection.rollback();
            throw e;
        } finally {
            dbConnection.setAutoCommit(true);
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
        List<Product> productsToInsert = new ArrayList<>();
        List<Integer> productsToDelete = new ArrayList<>();
        for (Map.Entry<Integer, Product> entry : dirtyProducts.entrySet()) {
            Product product = entry.getValue();
            dirtyProducts.remove(entry.getKey());
            productsToInsert.add(product);
        }

        for (Integer id : Set.copyOf(deletedProducts)) {
            deletedProducts.remove(id);
            productsToDelete.add(id);
        }
        insertProductsToDB(productsToInsert);
        deleteProductsFromDB(productsToDelete);
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

