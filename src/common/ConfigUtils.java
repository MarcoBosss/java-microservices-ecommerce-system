package common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigUtils {
    public static String loadConfigFile(String configFileName) throws IOException {
        return new String(Files.readAllBytes(Paths.get(configFileName)));
    }

    public static int getPort(String service, String config) {
        int start = config.indexOf(":", config.indexOf("\"port\"", config.indexOf(service))) + 1;
        int end = config.indexOf(",", start);
        return Integer.parseInt(config.substring(start, end).trim());
    }

    public static String getIp(String service, String config) {
        int start = config.indexOf("\"", config.indexOf(":", config.indexOf("\"ip\"", config.indexOf(service)))) + 1;
        int end = config.indexOf("\"", start);
        return config.substring(start, end).trim();
    }
}