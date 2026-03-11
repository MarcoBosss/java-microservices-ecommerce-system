import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import common.HttpResponse;
import common.ConfigUtils;
import common.DbUtils;
import common.HttpUtils;

public class ProductService {
    static HttpServer server;
    final static String dbPath = "src/db/product.db";
    static String dbIp;
    static int dbPort;
    static String dbName = "ecommerce";
    static String dbPassword = "password";

    public static void main(String[] args) throws IOException {
        // loadProducts(dbPath);
        String configFileName = ConfigUtils.loadConfigFile(args[0]);
        dbIp = ConfigUtils.getIp("Database", configFileName);
        dbPort = ConfigUtils.getPort("Database", configFileName);

        int port = ConfigUtils.getPort("ProductService", configFileName);
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(512)); // Adjust the pool size as needed
        server.createContext("/product", new ProductPostHandler());
        server.createContext("/product/", new ProductGetHandler());
        server.createContext("/shutdown", new ShutdownHandler());
        server.createContext("/wipe", new WipeHandler());
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
            HttpResponse response = getProduct(id);
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

    private static HttpResponse productCommand(String command, Product productData) {
        if ("create".equals(command)) {
            return createProduct(productData);
        } else if ("update".equals(command)) {
            return updateProduct(productData);
        } else if ("delete".equals(command)) {
            return deleteProduct(productData);
        }

        // service not found
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

    private static HttpResponse getProduct(int id) {
        String sql = "SELECT id, name, description, price, quantity FROM products WHERE id = ?";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            statement.setInt(1, id);

            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return new HttpResponse(200, getProductJsonBuilder(result));
                } else {
                    return new HttpResponse(404, "{}");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new HttpResponse(500, "{}");
    }    

    private static String getProductJsonBuilder(ResultSet result) throws SQLException{
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":").append(result.getInt("id")).append(",");
        json.append("\"name\":\"").append(result.getString("name")).append("\",");
        json.append("\"description\":\"").append(result.getString("description")).append("\",");
        json.append("\"price\":").append(String.format(Locale.US, "%.2f", result.getDouble("price"))).append(",");
        json.append("\"quantity\":").append(result.getInt("quantity"));
        json.append("}");
        return json.toString();
    }

    private static HttpResponse createProduct(Product productData) {
        if (checkProductExistence(productData.id)) {
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
        insertProduct(productData);
        return new HttpResponse(200, productToJson(productData));
    }

    private static HttpResponse updateProduct(Product productData) {
        // check product existence
        if (!checkProductExistence(productData.id)) {
            return new HttpResponse(404, "{}");
        }

        // build update command 
        StringBuilder sql = new StringBuilder("UPDATE products SET ");
        boolean productnameExist=false, descriptionExist=false, priceExist=false, quantityExist=false;
        boolean first = true;
        if (productData.productname != null) {
            if (productData.productname.equals("")) {
                return new HttpResponse(400, "{}");
            }
            sql.append("name = ?");
            first = false;
            productnameExist = true;
        }
        if (productData.description != null) {
            if (productData.description.equals("")) {
                return new HttpResponse(400, "{}");
            }
            if (!first) {
                sql.append(", ");
            }
            sql.append("description = ?");
            first = false;
            descriptionExist = true;
        }
        if (productData.price != null) {
            if (productData.price < 0) {
                return new HttpResponse(400, "{}");
            }
            if (!first) {
                sql.append(", ");
            }
            sql.append("price = ?");
            first = false;
            priceExist = true;
        }
        if (productData.quantity != null) {
            if (productData.quantity < 0) {
                return new HttpResponse(400, "{}");
            }
            if (!first) {
                sql.append(", ");
            }
            sql.append("quantity = ?");
            first = false;
            quantityExist = true;
        }
        if (!productnameExist && !descriptionExist && !priceExist && !quantityExist) {
            return getProduct(productData.id);
        }
        sql.append(" WHERE id = ? ");
        sql.append("RETURNING id, name, description, price, quantity ");

        // update product data
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql.toString())) {
            int i = 1;
            if (productnameExist) {
                statement.setString(i, productData.productname);
                i++;
            }
            if (descriptionExist) {
                statement.setString(i, productData.description);
                i++;
            }
            if (priceExist) {
                statement.setDouble(i, productData.price);
                i++;
            }
            if (quantityExist) {
                statement.setInt(i, productData.quantity);
                i++;
            }
            statement.setInt(i, productData.id);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return new HttpResponse(200, getProductJsonBuilder(result));
                } else {
                    return new HttpResponse(500, "{}");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }        
        return new HttpResponse(500, "{}");
    }

    private static HttpResponse deleteProduct(Product productData) {
        // check product existence
        if (!checkProductExistence(productData.id)) {
            return new HttpResponse(404, "{}");
        }

        // check if all fields exist
        if (productData.productname == null || productData.price == null || productData.quantity == null) {
            return new HttpResponse(400, "{}");
        }

        // delete if fields correspond
        String sql = "DELETE FROM products WHERE id = ? AND name = ? AND price = ? AND quantity = ?";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            
            statement.setInt(1, productData.id);
            statement.setString(2, productData.productname);
            statement.setDouble(3, productData.price);
            statement.setInt(4, productData.quantity);

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

    private static void insertProduct(Product product) {
        String sql = "INSERT INTO products (id, name, description, price, quantity) VALUES (?, ?, ?, ?, ?)";
        try (Connection dbConnection = DbUtils.getDBConnection(dbIp, dbPort, dbName, dbPassword);
            var statement = dbConnection.prepareStatement(sql)) {
            
            statement.setInt(1, product.id);
            statement.setString(2, product.productname);
            statement.setString(3, product.description);
            statement.setDouble(4, product.price);
            statement.setInt(5, product.quantity);

            statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean checkProductExistence(int id) {
        String sql = "SELECT 1 FROM products WHERE id = ? LIMIT 1";
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

    private static String productToJson(Product product) {
        String json = "{" + "\"id\":" + product.id + "," + 
            "\"name\":" + "\"" + product.productname + "\"" + "," +
            "\"description\":" + "\"" + product.description + "\"" + "," +
            "\"price\":" + String.format(Locale.US, "%.2f", product.price) + "," +
            "\"quantity\":" + product.quantity +
            "}";
        return json;
    }

}

