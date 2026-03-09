import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class ProductService {
    // data base
    static Map<Integer, Product> db = new ConcurrentHashMap<>();
    static HttpServer server;
    final static String dbPath = "src/db/product.db";

    public static void main(String[] args) throws IOException {
        loadProducts(dbPath);
        int port = getPort("ProductService", args[0]);
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(20)); // Adjust the pool size as needed
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
            // Handle POST request for /test
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = getRequestBody(exchange);
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                if (contentType == null || !contentType.equals("application/json")) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    exchange.close();
                    return;
                }                
                Map<String, String> json = stringToJSON(body);
                if (json == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    exchange.close();   
                    return;                 
                }

                // command types
                String command = json.get("command");
                Product productData = jsonToProduct(json);
                if (productData == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, 0);
                    exchange.close();
                    return;
                }
                int status;
                if ("create".equals(command)) {
                    status = createProduct(productData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    if (status == 200) {
                        sendResponse(exchange, productToJson(db.get(productData.id)));
                    } else {
                        sendResponse(exchange, "{}");
                    }
                } else if ("update".equals(command)) {
                    status = updateProduct(productData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    if (status == 200) {
                        sendResponse(exchange, productToJson(db.get(productData.id)));
                    } else {
                        sendResponse(exchange, "{}");
                    }
                } else if ("delete".equals(command)) {
                    status = deleteProduct(productData);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, 0);
                    sendResponse(exchange, "{}");
                } else {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, 0);
                }
            } else {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, 0);
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


        private static Integer createProduct(Product productData) {
            if (db.get(productData.id) != null) {
                return 409;
            }
            if (productData.productname == null || productData.price == null || productData.quantity == null || productData.description == null) {
                return 400;
            }
            if (productData.productname.equals("") || productData.description.equals("")) {
                return 400;
            }
            if (productData.quantity < 0 || productData.price < 0) {
                return 400;
            }
            db.put(productData.id, productData);
            return 200;
        }

        private static Integer updateProduct(Product productData) {
            Product product = db.get(productData.id);

            // check if product exists
            if (product == null) {
                return 404;
            }

            // update product data
            if (productData.productname != null) {
                if (productData.productname.equals("")) {
                    return 400;
                }
                product.productname = productData.productname;
            }
             if (productData.description != null) {
                if (productData.description.equals("")) {
                    return 400;
                }
                product.description = productData.description;
            }           
            if (productData.price != null) {
                if (productData.price < 0) {
                    return 400;
                }
                product.price = productData.price;
            }
            if (productData.quantity != null) {
                if (productData.quantity < 0) {
                    return 400;
                }
                product.quantity = productData.quantity;
            }
            return 200;
        }

        private static Integer deleteProduct(Product productData) {
            // check if product exists
            Product product = db.get(productData.id);
            if (product == null) {
                return 404;
            }

            // check if all fields exist
            if (productData.productname == null || productData.price == null || productData.quantity == null) {
                return 400;
            }

            // delete if fields correspond
            if (productData.productname.equals(product.productname) && productData.price.equals(product.price) && productData.quantity.equals(product.quantity)) {
                db.remove(productData.id);
                return 200;
            }
            return 401;
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

    static class ProductGetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle POST request for /test
            if ("GET".equals(exchange.getRequestMethod())) {
                // try to parse id
                Integer id;
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
                Product product = db.get(id);
                if (product == null) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, 0);
                    sendResponse(exchange, "{}");
                    exchange.close();
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                sendResponse(exchange, productToJson(product));
                exchange.close();
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
                storeProducts(dbPath);
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

    private static String productToJson(Product product) {
        String json = "{" + "\"id\":" + product.id + "," + 
            "\"name\":" + "\"" + product.productname + "\"" + "," +
            "\"description\":" + "\"" + product.description + "\"" + "," +
            "\"price\":" + String.format(Locale.US, "%.2f", product.price) + "," +
            "\"quantity\":" + product.quantity +
            "}";
        return json;
    }

    private static void loadProducts(String pathName) throws IOException, FileNotFoundException {
        File file = new File(pathName);
        file.createNewFile();
        
        // load products
        Scanner reader = new Scanner(file);
        while (reader.hasNextLine()) {
            String productString = reader.nextLine();
            if (productString.isEmpty()) {
                continue;
            }
            String productData[] = productString.trim().split("\\|");
            int product_id = Integer.parseInt(productData[0]);
            Product product = new Product(product_id, productData[1], productData[2], Float.parseFloat(productData[3]), Integer.parseInt(productData[4]));
            db.put(product_id, product);
        }
        reader.close();
    }

    private static void storeProducts(String pathName) throws IOException, FileNotFoundException {
        // store products
        FileWriter writer = new FileWriter(pathName);
        for (Product product : db.values()) {
            StringBuilder productData = new StringBuilder();
            productData.append(product.id);
            productData.append("|");
            productData.append(product.productname);
            productData.append("|");
            productData.append(product.description);
            productData.append("|");
            productData.append(product.price);
            productData.append("|");
            productData.append(product.quantity);
            productData.append("\n");
            writer.write(productData.toString());
        }
        writer.close();
    }

    private static void wipeDB(String pathName) throws IOException {
        new FileWriter(pathName, false).close();
    }
    
}

