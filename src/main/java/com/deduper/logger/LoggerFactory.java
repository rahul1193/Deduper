package com.deduper.logger;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

/**
 * authored by @rahulanishetty on 7/24/16.
 */
public class LoggerFactory {

    public static final ILoggerFactory loggerFactory = org.slf4j.LoggerFactory.getILoggerFactory();

    public static Logger getLogger(Class<?> clz) {
        return loggerFactory.getLogger(clz.getName());
    }
}
