package com.bilik.domain;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

@JsonPropertyOrder(value = {"timestamp", "randomId", "userId", "campaignId", "groupId", "adId", "urlHash", "domainHash", "zoneId", "feedtag", "clicked"})
public class Result implements Serializable {

    private long timestamp;
    private long randomId;
    private int userId;
    private int campaignId;
    private int groupId;
    private int adId;
    private long urlHash;
    private long domainHash;
    private int zoneId;
    private byte feedtag;
    private boolean clicked;

    public Result() {}

    public Result(Feedback feedback, Impression impression) {
        this.timestamp = feedback.getTimestamp();
        this.randomId = feedback.getRandomId();
        this.userId = impression.getUserId();
        this.campaignId = impression.getCampaignId();
        this.groupId = impression.getGroupId();
        this.adId = impression.getAdId();
        this.urlHash = feedback.getUrlHash();
        this.domainHash = feedback.getDomainHash();
        this.zoneId = feedback.getZoneId();
        this.feedtag = feedback.getFeedtag();
    }

    public Result(long timestamp, int randomId, int userId, int campaignId, int groupId, int adId, long urlHash, long domainHash, int zoneId, byte feedtag, boolean clicked) {
        this.timestamp = timestamp;
        this.randomId = randomId;
        this.userId = userId;
        this.campaignId = campaignId;
        this.groupId = groupId;
        this.adId = adId;
        this.urlHash = urlHash;
        this.domainHash = domainHash;
        this.zoneId = zoneId;
        this.feedtag = feedtag;
        this.clicked = clicked;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getRandomId() {
        return randomId;
    }

    public void setRandomId(long randomId) {
        this.randomId = randomId;
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

    public boolean isClicked() {
        return clicked;
    }

    public void setClicked(boolean clicked) {
        this.clicked = clicked;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Result result = (Result) o;
        return timestamp == result.timestamp && randomId == result.randomId && userId == result.userId && campaignId == result.campaignId && groupId == result.groupId && adId == result.adId && urlHash == result.urlHash && domainHash == result.domainHash && zoneId == result.zoneId && feedtag == result.feedtag && clicked == result.clicked;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, randomId, userId, campaignId, groupId, adId, urlHash, domainHash, zoneId, feedtag, clicked);
    }

    @Override
    public String toString() {
        return new StringJoiner(",")
                .add(String.valueOf(timestamp))
                .add(String.valueOf(randomId))
                .add(String.valueOf(userId))
                .add(String.valueOf(campaignId))
                .add(String.valueOf(groupId))
                .add(String.valueOf(adId))
                .add(String.valueOf(urlHash))
                .add(String.valueOf(domainHash))
                .add(String.valueOf(zoneId))
                .add(String.valueOf(feedtag))
                .add(String.valueOf(clicked))
                .toString();
    }
}
