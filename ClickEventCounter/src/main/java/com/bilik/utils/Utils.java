package com.bilik.utils;

import java.time.LocalDateTime;

public class Utils {

    public static void log(Object s) {
        System.out.println("{" + LocalDateTime.now() + "][" + Thread.currentThread().getName() + "]# " + s.toString());
    }

}