package com.bilik.TaxiRideWindowing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class Ride {

    private long rideId;
    private long taxiId;
    private boolean started;
    private float lon, lat;
    private int psgCnt;
    private Date rideTime;

    public Ride() {}

    public Ride(long rideId, long taxiId, boolean started, float lon, float lat, int psgCnt, Date rideTime) {
        this.rideId = rideId;
        this.taxiId = taxiId;
        this.started = started;
        this.lon = lon;
        this.lat = lat;
        this.psgCnt = psgCnt;
        this.rideTime = rideTime;
    }

    public long getRideId() {
        return rideId;
    }

    public void setRideId(long rideId) {
        this.rideId = rideId;
    }

    public long getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(long taxiId) {
        this.taxiId = taxiId;
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public float getLon() {
        return lon;
    }

    public void setLon(float lon) {
        this.lon = lon;
    }

    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public int getPsgCnt() {
        return psgCnt;
    }

    public void setPsgCnt(int psgCnt) {
        this.psgCnt = psgCnt;
    }

    public Date getRideTime() {
        return rideTime;
    }

    public void setRideTime(Date rideTime) {
        this.rideTime = rideTime;
    }

    public static Collection<Ride> createRides() {
        Collection<Ride> col = new ArrayList<>(100);
        long start = System.currentTimeMillis();
        for (int i = 1; i <= 100; i++) {
            col.add(new Ride(
                    i,
                    ThreadLocalRandom.current().nextInt(100, 110),
                    true,
                    4325f + i,
                    983943f + i,
                    ThreadLocalRandom.current().nextInt(4),
                    new Date(start += 1000)
            ));
        }
        return col;
    }

    @Override
    public String toString() {
        return "Ride{" +
                "rideId=" + rideId +
                ", taxiId=" + taxiId +
                ", started=" + started +
                ", lon=" + lon +
                ", lat=" + lat +
                ", psgCnt=" + psgCnt +
                ", rideTime=" + rideTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ride ride = (Ride) o;
        return rideId == ride.rideId && taxiId == ride.taxiId && started == ride.started && Float.compare(ride.lon, lon) == 0 && Float.compare(ride.lat, lat) == 0 && psgCnt == ride.psgCnt && Objects.equals(rideTime, ride.rideTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rideId, taxiId, started, lon, lat, psgCnt, rideTime);
    }
}
