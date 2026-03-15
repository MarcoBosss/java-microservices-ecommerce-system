import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import common.HttpResponse;
import common.HttpUtils;
import common.JsonUtils;

public class LoadBalancer {
    // service info
    final static int port = 13000;
    final static int healthCheckTime = 5;
    static HttpServer server;
    static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // other serivices info
    static AtomicInteger orderId = new AtomicInteger(0);
    static List<JsonUtils.Service> userServices = new ArrayList<>();
    static List<JsonUtils.Service> productServices = new ArrayList<>();
    static List<JsonUtils.Service> orderServices = new ArrayList<>();



    public static void main(String[] args) throws IOException {
        // load servers credentials
        String configFileName = args[0];
        loadServices(configFileName);
        Runnable task = () -> {
            try {
                healthCheck(userServices);
                healthCheck(productServices);
                healthCheck(orderServices);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        scheduler.scheduleAtFixedRate(task, 0, healthCheckTime, TimeUnit.SECONDS);

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(256)); 
        server.createContext("/user/purchased", new UserPurchasedHandler());
        server.createContext("/user", new UserHandler());
        server.createContext("/product", new ProductHandler());
        server.createContext("/order", new OrderHandler());
        server.start();
    }

    static class UserPurchasedHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleHelper(exchange, orderServices);  
        }
    }

    static class UserHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleHelper(exchange, userServices); 
        }
    }

    static class ProductHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleHelper(exchange, productServices);
        }
    }

    static class OrderHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String body = HttpUtils.getRequestBody(exchange);
            String requestURI = exchange.getRequestURI().toString();
            String requestMethod = exchange.getRequestMethod();

            // assign order id
            int order_id = orderId.getAndIncrement();           
            body = "{\"id\":" + order_id + "," + body.substring(1);
            
            Map<String, String> json = HttpUtils.stringToJSON(body);
            int id = extractId(requestMethod, requestURI, json);
            JsonUtils.Service service = selectService(id, orderServices);
            if (service == null) {
                HttpUtils.sendJsonResponse(exchange, 503, ""); 
                return;
            }
            try {
                HttpResponse response = HttpUtils.forwardRequest(
                    service.ip, 
                    service.port, 
                    requestURI, 
                    requestMethod, 
                    body
                );
                HttpUtils.sendJsonResponse(exchange, response.status, response.body);   
            } catch (Exception e) {
                System.out.println(service.type + " request failed: " + service.ip + ":" + service.port);
                HttpUtils.sendJsonResponse(exchange, 503, "{}");
            }
        }
    }

    private static void healthCheck(List<JsonUtils.Service> services) throws IOException {
        // heath check for main services
        JsonUtils.Service service;
        for (int i=0;i<services.size()-1;i++) {
            service = services.get(i);
            try {
                HttpResponse response = HttpUtils.forwardRequest(
                    service.ip, 
                    service.port, 
                    "/livecheck", 
                    "GET", 
                    ""
                );
                service.isAlive = (response.status == 200);
                if (!service.isAlive)  {
                    replaceService(i, services);
                }
            } catch (Exception e) {
                service.isAlive = false;
                replaceService(i, services);
                System.out.println(
                    service.type + " unavailable: " + service.ip);
            }
        }

        // health check for backup service
        service = services.get(services.size()-1);
        try {
            HttpResponse response = HttpUtils.forwardRequest(
                service.ip, 
                service.port, 
                "/livecheck", 
                "GET", 
                ""
            );
            service.isAlive = (response.status == 200);
        } catch (Exception e) {
            service.isAlive = false;
            System.out.println(
                service.type + " backup unavailable: " + service.ip);
        }
    }

    private static void handleHelper(HttpExchange exchange, List<JsonUtils.Service> services) throws IOException {
        String body = HttpUtils.getRequestBody(exchange);
        String requestURI = exchange.getRequestURI().toString();
        String requestMethod = exchange.getRequestMethod();
        Map<String, String> json = HttpUtils.stringToJSON(body);
        int id = extractId(requestMethod, requestURI, json);
        JsonUtils.Service service = selectService(id, services);
        if (service == null) {
            HttpUtils.sendJsonResponse(exchange, 503, ""); 
            return;
        }
        try {
            HttpResponse response = HttpUtils.forwardRequest(
                service.ip, 
                service.port, 
                requestURI, 
                requestMethod, 
                body
            );
            HttpUtils.sendJsonResponse(exchange, response.status, response.body);   
        } catch (Exception e) {
            System.out.println(service.type + " request failed: " + service.ip + ":" + service.port);
            HttpUtils.sendJsonResponse(exchange, 503, "{}");
        }

    }

    private static int extractId(String method, String URI, Map<String, String> json) {
        // return user id for order services
        if (URI.contains("/order")) {
            if (json != null && json.containsKey("user_id")) {
                try {
                    return Integer.parseInt(json.get("user_id"));
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
            return 0;
        }

        // get requests have id in URI
        if ("GET".equals(method)) {
            try {
                String[] tokens = URI.split("/");
                return Integer.parseInt(tokens[tokens.length - 1]);
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        // post requests have id in body
        if (json != null && json.containsKey("id")) {
            try {
                return Integer.parseInt(json.get("id"));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    private static JsonUtils.Service selectService(int id, List<JsonUtils.Service> services) {
        int index = Math.floorMod(id * 0x9E3779B9, services.size() - 1);
        return services.get(index);
    }

    private static void replaceService(int index, List<JsonUtils.Service> services) throws IOException {
        JsonUtils.Service failed = services.get(index);
        JsonUtils.Service backup = services.get(services.size()-1);
        if (!backup.isAlive) {
            System.out.println(failed.type + " no backup available");
            return;
        }

        services.set(index, backup);
        services.set(services.size() - 1, failed);
        HttpUtils.forwardRequestNoResponse(
            failed.ip,
            failed.port,
            "/standby",
            "POST",
            ""
        );
        System.out.println(failed.type + " replaced");
        if (failed.type.equals("UserService")) {
            notifyOrder(index, "user");
        } else if (failed.type.equals("ProductService")){
            notifyOrder(index, "product");
        }
    }

    private static void notifyOrder(int index, String type) throws IOException {
        for (JsonUtils.Service orderService: orderServices) {
            HttpUtils.forwardRequestNoResponse(
                orderService.ip,
                orderService.port,
                "/standby/" + type + "/"+ index,
                "POST",
                ""
            );
        }
    }

    private static void loadServices(String configFileName) throws FileNotFoundException {
        JsonUtils.Config config = new Gson().fromJson(new FileReader(configFileName), JsonUtils.Config.class);
        for (JsonUtils.ServiceConfig service : config.UserServices) {
            userServices.add(new JsonUtils.Service("UserService", service.ip, service.port));
        }

        for (JsonUtils.ServiceConfig service : config.ProductServices) {
            productServices.add(new JsonUtils.Service("ProductService", service.ip, service.port));
        }

        for (JsonUtils.ServiceConfig service : config.OrderServices) {
            orderServices.add(new JsonUtils.Service("OrderService", service.ip, service.port));
        }
    }
}