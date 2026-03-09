import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.FileReader;
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

public class Parser {
    public static void main(String[] args) throws IOException {
        // get ports and ips
        int OSPort = getPort("OrderService", args[0]);
        String OSIp = getIp("OrderService", args[0]);

        // get wordload file
        String workload = args[1];

        // parse
        try (BufferedReader br = new BufferedReader(new FileReader(workload))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(" ");
                String body;
                if ("USER".equals(tokens[0])) {
                    if ("create".equals(tokens[1])) {
                        body = "{" + "\"command\":\"create\"," +
                        "\"id\":" + tokens[2] + "," + 
                        "\"username\":" + "\"" + tokens[3] + "\"" + "," +
                        "\"email\":" + "\"" + tokens[4] + "\"" + "," +
                        "\"password\":" + "\"" + tokens[5] + "\"" +
                        "}";
                        forwardRequest(OSIp, OSPort, "/user", "POST", body);
                    } else if ("update".equals(tokens[1])) {
                        body = "{" + "\"command\":\"update\"" + 
                        "," + "\"id\":" + tokens[2];
                        for (int i=3;i<tokens.length;i++) {
                            String[] token = tokens[i].split(":");
                            if ("username".equals(token[0])) {
                                body = body + "," + "\"username\":" + "\"" + token[1] + "\"";
                            } else if ("email".equals(tokens[i].split(":")[0])) {
                                body = body + "," + "\"email\":" + "\"" + token[1] + "\"";
                            } else if ("password".equals(tokens[i].split(":")[0])) {
                                body = body + "," + "\"password\":" + "\"" + token[1] + "\"";
                            }
                        }
                        body = body + "}";
                        forwardRequest(OSIp, OSPort, "/user", "POST", body);
                    } else if ("delete".equals(tokens[1])) {
                        body = "{" + "\"command\":\"delete\"," +
                        "\"id\":" + tokens[2] + "," + 
                        "\"username\":" + "\"" + tokens[3] + "\"" + "," +
                        "\"email\":" + "\"" + tokens[4] + "\"" + "," +
                        "\"password\":" + "\"" + tokens[5] + "\"" +
                        "}";
                        forwardRequest(OSIp, OSPort, "/user", "POST", body);
                    } else if ("get".equals(tokens[1])) {
                        forwardRequest(OSIp, OSPort, "/user/"+tokens[2], "GET", "");
                    }
                } else if ("PRODUCT".equals(tokens[0])) {
                    if ("create".equals(tokens[1])) {
                        body = "{" + "\"command\":\"create\"," +
                        "\"id\":" + tokens[2] + "," + 
                        "\"name\":" + "\"" + tokens[3] + "\"" + "," +
                        "\"description\":" + "\"" + tokens[4] + "\"" + "," +
                        "\"price\":" +  tokens[5] + "," +
                        "\"quantity\":" + tokens[6] +
                        "}";
                        forwardRequest(OSIp, OSPort, "/product", "POST", body);
                    } else if ("update".equals(tokens[1])) {
                        body = "{" + "\"command\":\"update\"" + 
                        "," + "\"id\":" + tokens[2];
                        for (int i=3;i<tokens.length;i++) {
                            String[] token = tokens[i].split(":");
                            if ("name".equals(token[0])) {
                                body = body + "," + "\"name\":" + "\"" + token[1] + "\"";
                            } else if ("description".equals(token[0])) {
                                body = body + "," + "\"description\":" + "\"" + token[1] + "\"";
                            } else if ("price".equals(tokens[i].split(":")[0])) {
                                body = body + "," + "\"price\":" + token[1];
                            } else if ("quantity".equals(tokens[i].split(":")[0])) {
                                body = body + "," + "\"quantity\":" + token[1];
                            }
                        }
                        body = body + "}";
                        forwardRequest(OSIp, OSPort, "/product", "POST", body);
                    } else if ("delete".equals(tokens[1].toLowerCase())) {
                        body = "{" + "\"command\":\"delete\"," +
                        "\"id\":" + tokens[2] + "," + 
                        "\"name\":" + "\"" + tokens[3] + "\"" + "," +
                        "\"price\":" +  tokens[4] + "," +
                        "\"quantity\":" + tokens[5] +
                        "}";
                        forwardRequest(OSIp, OSPort, "/product", "POST", body);
                    } else if ("info".equals(tokens[1])) {
                        forwardRequest(OSIp, OSPort, "/product/"+tokens[2], "GET", "");
                    }
                } else if ("ORDER".equals(tokens[0])) {
                    if ("place".equals(tokens[1])) {
                        body = "{" + "\"command\":\"place order\"," +
                        "\"product_id\":" + tokens[2] + "," + 
                        "\"user_id\":" + tokens[3] + "," + 
                        "\"quantity\":" + tokens[4] + 
                        "}";
                        forwardRequest(OSIp, OSPort, "/order", "POST", body);
                    }
                } else if ("shutdown".equals(tokens[0])) {
                    forwardRequest(OSIp, OSPort, "/shutdown", "POST", "");
                } else if ("restart".equals(tokens[0])) {
                    forwardRequest(OSIp, OSPort, "/restart", "POST", "");
                } 
            }
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

    @SuppressWarnings("deprecation")
    private static Result forwardRequest(String ip, int port, String requestURI, String requestMethod, String body) throws IOException {
        // set up connection
        String urlString = "http://" + ip + ":" + port + requestURI;
        URL url = new URL(urlString);
        System.out.println("Forwarding to: " + urlString);
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
        System.out.println("Forwarding to: " + urlString);
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
        System.out.println("Forwarding to: " + urlString);
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

    private static void sendResponse(HttpExchange exchange, String response, int status) throws IOException {
        exchange.sendResponseHeaders(status, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes(StandardCharsets.UTF_8));
        os.close();
    }

    private static void sendResponseBytes(HttpExchange exchange, byte[] response, int status) throws IOException {
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
}

