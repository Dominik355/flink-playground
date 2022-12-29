package com.bilik.broadcastStatePattern;

import java.util.Collection;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Item {

    public Shape shape;
    public Color color;

    public Item() {
        this.shape = Shape.values()[ThreadLocalRandom.current().nextInt(Shape.values().length)];
        this.color = Color.values()[ThreadLocalRandom.current().nextInt(Color.values().length)];
    }

    public Shape getShape() {
        return shape;
    }

    public String getColor() {
        return color.name();
    }

    public static enum Shape {
        TRIANGLE, RECTANGLE
    }

    public static Collection<Item> generateItems(int size) {
        return IntStream.range(0, size)
                .mapToObj(i -> new Item())
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Item.class.getSimpleName() + "[", "]")
                .add("shape=" + shape.name())
                .add("color=" + color.name())
                .toString();
    }
}
