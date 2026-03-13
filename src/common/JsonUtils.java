package common;

import java.util.List;

import common.JsonUtils.ServiceConfig;

public class JsonUtils {
    public static class ServiceConfig {
        public String ip;
        public int port;
    }

    public static class Config {
        public List<ServiceConfig> UserServices;
        public List<ServiceConfig> ProductServices;
        public List<ServiceConfig> OrderServices;
        public ServiceConfig UserDatabase;
        public ServiceConfig ProductDatabase;
        public ServiceConfig OrderDatabase;
    }

    public static class Service {
        public final String type;
        public final String ip;
        public final int port;
        public volatile boolean isAlive;

        public Service(String type, String ip, int port) {
            this.type = type;
            this.ip = ip;
            this.port = port;
            this.isAlive = true;
        }
    }
}