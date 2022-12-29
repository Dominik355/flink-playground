package com.bilik.broadcastStatePattern;

import com.bilik.broadcastStatePattern.Item.Shape;

/**
 * We wanna rectangle followed by a triangle, so first rule is a Rectangle and second is a Triangle
 */
public class ShapeRule {

    public String name;

    public Shape first;
    public Shape second;

    public ShapeRule(Shape first, Shape second, String name) {
        this.first = first;
        this.second = second;
        this.name = name;
    }

    public ShapeRule() {
        this.first = Shape.RECTANGLE;
        this.second = Shape.TRIANGLE;
        this.name = "Basic Shape Rule";
    }
}
