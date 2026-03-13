package common;

import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HttpUtils {
    public static String getRequestBody(HttpExchange exchange) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            StringBuilder requestBody = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                requestBody.append(line);
            }
            return requestBody.toString();
        }
    }

    public static void sendJsonResponse(HttpExchange exchange, int status, String body) throws IOException{
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
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

    @SuppressWarnings("deprecation")
    public static HttpResponse forwardRequest(String ip, int port, String requestURI, String requestMethod, String body) throws IOException {
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
        return new HttpResponse(status, new String(response, StandardCharsets.UTF_8));
    }

    @SuppressWarnings("deprecation")
    public static void forwardRequestNoResponse(String ip, int port, String requestURI, String requestMethod, String body) throws IOException {
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
    }

    @SuppressWarnings("deprecation")
    public static Integer forwardRequstStatus(String ip, int port, String requestURI, String requestMethod) throws IOException {
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
    public static String getBody(String ip, int port, String requestURI, String requestMethod) throws IOException {
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
}