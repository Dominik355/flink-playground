package com.bilik.common.utils;

import java.time.LocalDateTime;

public class Logger {

    private final Class clasz;

    public Logger(Class clasz) {
        this.clasz = clasz;
    }

    public void info(String log, Object... params) {
        logDefault(log, params);
    }

    public void debug(String log, Object... params) {
        logDefault(log, params);
    }

    public void error(String log, Object... params) {
        logDefault(log, params);
    }

    private void logDefault(String log, Object... params) {
        System.out.println(getDefault() + String.format(log.replace("{}", "%s"), params));
    }

    private String getDefault() {
        return "{" + LocalDateTime.now() + "][" + Thread.currentThread().getName() + "]#" + clasz.getCanonicalName() + " - ";
    }

    public <T> T println(String log, T obj) {
        System.out.println(log + obj);
        return obj;
    }

}
