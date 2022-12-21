package com.bilik.utils;

import java.util.ArrayList;
import java.util.List;

public class ListUtils {

    public static <E> List<E> listWithElements(E... input) {
        List<E> list = new ArrayList<>();
        for (int i = 0; i < input.length; i++) {
            list.add(input[i]);
        }
        return list;
    }

}
