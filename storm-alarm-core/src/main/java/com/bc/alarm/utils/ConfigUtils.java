package com.bc.alarm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置工具类
 *
 * @author zhou
 */
public class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static volatile Properties PROPERTIES;

    public static String getProperty(String key, String defaultValue) {
        Properties properties = getProperties();
        String value = properties.getProperty(key);
        if (null != value) {
            return value.trim();
        }
        return defaultValue;
    }

    public static Boolean getBooleanProperty(String key, Boolean defaultValue) {
        Properties properties = getProperties();
        String value = properties.getProperty(key);

        if (null != value) {
            return Boolean.valueOf(value.trim());
        }
        return defaultValue;
    }

    public static Properties getProperties() {
        if (null == PROPERTIES) {
            synchronized (ConfigUtils.class) {
                if (null == PROPERTIES) {
                    PROPERTIES = ConfigUtils.loadProperties();
                }
            }
        }
        return PROPERTIES;
    }

    public static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream input = null;
        try {
            try {
                input = ConfigUtils.class.getClassLoader().getResourceAsStream("mongodb.properties");
                properties.load(input);
            } finally {
                input.close();
            }
        } catch (Throwable e) {
            logger.warn("Failed to load canal.properties");
        }
        return properties;
    }
}