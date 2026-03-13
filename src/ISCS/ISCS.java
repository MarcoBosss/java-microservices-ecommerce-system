import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class ISCS {
    static String configFileName;
    static HttpServer server;

    public static void main(String[] args) throws IOException {
        configFileName = args[0];
        int port = 14000;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(2048)); // Adjust the pool size as needed
        server.createContext("/", new ISCSHandler());
        server.createContext("/shutdown", new ShutdownHandler());
        server.createContext("/wipe", new WipeHandler());
        server.start();
    }

    static class ISCSHandler implements HttpHandler {
        @SuppressWarnings("deprecation")
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String clientAddress = exchange.getRemoteAddress().getAddress().toString();
            String requestMethod = exchange.getRequestMethod();
            String requestURI = exchange.getRequestURI().toString();
            String path = exchange.getRequestURI().getPath();
            
            // determine service
            String service;
            if ("user".equals(path.split("/")[1])) {
                service = "UserService";
            } else if ("product".equals(path.split("/")[1])) {
                service = "ProductService";
            } else {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, 0);
                exchange.close();
                return;              
            } 

            // get corresponding ip and port from config.json
            String ip = getIp(service, configFileName);
            int port = getPort(service, configFileName);

            // set up connection
            String urlString = "http://" + ip + ":" + port + requestURI;
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(requestMethod);

            // forward body if POST
            if ("POST".equals(requestMethod)) {
                String body = getRequestBody(exchange);
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                OutputStream os = connection.getOutputStream();
                os.write(body.getBytes(StandardCharsets.UTF_8));
                os.close();
            }

            // forward response
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
            sendResponseBytes(exchange, response, status);

            // clean up
            connection.disconnect();
            exchange.close();

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

    static class ShutdownHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // shutdown user and product services
                forwardRequest(
                    getIp("UserService", configFileName), 
                    getPort("UserService", configFileName),
                    "/shutdown", "POST", "");
                forwardRequest(
                    getIp("ProductService", configFileName), 
                    getPort("ProductService", configFileName),
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

    static class WipeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // shutdown user and product services
                forwardRequest(
                    getIp("UserService", configFileName), 
                    getPort("UserService", configFileName),
                    "/wipe", "POST", "");
                forwardRequest(
                    getIp("ProductService", configFileName), 
                    getPort("ProductService", configFileName),
                    "/wipe", "POST", "");
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
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

    private static void sendResponseBytes(HttpExchange exchange, byte[] response, int status) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        OutputStream os = exchange.getResponseBody();
        os.write(response);
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

    static class Result {
        int status;
        String body;

        Result(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }

    @SuppressWarnings("deprecation")
    private static Result forwardRequest(String ip, int port, String requestURI, String requestMethod, String body) throws IOException {
        // set up connection
        String urlString = "http://" + ip + ":" + port + requestURI;
        URL url = new URL(urlString);
        //System.out.println("Forwarding to: " + urlString);
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
}

