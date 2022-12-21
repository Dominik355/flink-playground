package com.bilik.domain;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

@JsonPropertyOrder(value = {"timestamp", "randomId", "urlHash", "domainHash", "zoneId", "feedtag"})
public class Feedback implements Serializable {

    private long randomId;
    private long timestamp;
    private long urlHash;
    private long domainHash;
    private int zoneId;
    private byte feedtag;

    public Feedback() {}

    public Feedback(long randomId, long timestamp, long urlHash, long domainHash, int zoneId, byte feedtag) {
        this.randomId = randomId;
        this.timestamp = timestamp;
        this.urlHash = urlHash;
        this.domainHash = domainHash;
        this.zoneId = zoneId;
        this.feedtag = feedtag;
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

    public long getUrlHash() {
        return urlHash;
    }

    public void setUrlHash(long urlHash) {
        this.urlHash = urlHash;
    }

    public long getDomainHash() {
        return domainHash;
    }

    public void setDomainHash(long domainHash) {
        this.domainHash = domainHash;
    }

    public int getZoneId() {
        return zoneId;
    }

    public void setZoneId(int zoneId) {
        this.zoneId = zoneId;
    }

    public byte getFeedtag() {
        return feedtag;
    }

    public void setFeedtag(byte feedtag) {
        this.feedtag = feedtag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Feedback feedback = (Feedback) o;
        return randomId == feedback.randomId && timestamp == feedback.timestamp && urlHash == feedback.urlHash && domainHash == feedback.domainHash && zoneId == feedback.zoneId && feedtag == feedback.feedtag;
    }

    @Override
    public int hashCode() {
        return Objects.hash(randomId, timestamp, urlHash, domainHash, zoneId, feedtag);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Feedback.class.getSimpleName() + "[", "]")
                .add("randomId=" + randomId)
                .add("timestamp=" + timestamp)
                .add("urlHash=" + urlHash)
                .add("domainHash=" + domainHash)
                .add("zoneId=" + zoneId)
                .add("feedtag=" + feedtag)
                .toString();
    }
}
