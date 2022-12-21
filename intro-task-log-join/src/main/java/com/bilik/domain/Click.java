package com.bilik.domain;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;

@JsonPropertyOrder(value = {"timestamp", "randomId"})
public class Click implements Serializable {

    private long randomId;
    private long timestamp;

    public Click() {}

    public Click(long randomId, long timestamp) {
        this.randomId = randomId;
        this.timestamp = timestamp;
    }

    public long getRandomId() {
        return randomId;
    }

    public void setRandomId(long randomId) {
        this.randomId = randomId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Click click = (Click) o;
        return randomId == click.randomId && timestamp == click.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(randomId, timestamp);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Click{");
        sb.append("randomId=").append(randomId);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }
}
