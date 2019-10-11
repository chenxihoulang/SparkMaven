package com.chw.java.spark.count.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class ConfigUtils {
    final static String resourceFullName = "config.properties";
    final static Properties props = new Properties();

    static {
        InputStreamReader is = null;
        try {
            InputStream resourceAsStream = ConfigUtils.class.getResourceAsStream(resourceFullName);
            System.out.println(resourceAsStream);//null

            resourceAsStream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceFullName);
            System.out.println(resourceAsStream);//null

            resourceAsStream = new FileInputStream("src/main/resources/config.properties");

            is = new InputStreamReader(resourceAsStream, "utf-8");
            props.load(is);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String getConfig(String key) {
        return props.getProperty(key);
    }

    public static boolean getBooleanValue(String key) {
        if (props.get(key).equals("true")) {
            return true;
        } else {
            return false;
        }
    }

    public static Integer getIntValue(String key) {
        return Integer.parseInt(getConfig(key));
    }

    public static void main(String[] args) {
        ConfigUtils configUtils = new ConfigUtils();
        final String url = ConfigUtils.getConfig("url");
        System.out.println(url);
    }
}
