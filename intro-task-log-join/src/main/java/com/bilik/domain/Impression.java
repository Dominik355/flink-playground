package com.bilik.domain;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

@JsonPropertyOrder(value = {"timestamp", "randomId", "userId", "campaignId", "groupId", "adId"})
public class Impression implements Serializable {

    private long randomId;
    private long timestamp;
    private int userId;
    private int campaignId;
    private int groupId;
    private int adId;

    public Impression() {}

    public Impression(long randomId, long timestamp, int userId, int campaignId, int groupId, int adId) {
        this.randomId = randomId;
        this.timestamp = timestamp;
        this.userId = userId;
        this.campaignId = campaignId;
        this.groupId = groupId;
        this.adId = adId;
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

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(int campaignId) {
        this.campaignId = campaignId;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public int getAdId() {
        return adId;
    }

    public void setAdId(int adId) {
        this.adId = adId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Impression imps = (Impression) o;
        return randomId == imps.randomId && timestamp == imps.timestamp && userId == imps.userId && campaignId == imps.campaignId && groupId == imps.groupId && adId == imps.adId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(randomId, timestamp, userId, campaignId, groupId, adId);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Impression.class.getSimpleName() + "[", "]")
                .add("randomId=" + randomId)
                .add("timestamp=" + timestamp)
                .add("userId=" + userId)
                .add("campaignId=" + campaignId)
                .add("groupId=" + groupId)
                .add("adId=" + adId)
                .toString();
    }
}
