package com.bilik.rides_and_fares;

import java.time.Instant;

import com.bilik.common.datatypes.*;

public class RidesAndFaresTestBase {

    public static TaxiRide testRide(long rideId) {
        return new TaxiRide(rideId, true, Instant.EPOCH, 0F, 0F, 0F, 0F, (short) 1, 0, rideId);
    }

    public static TaxiFare testFare(long rideId) {
        return new TaxiFare(rideId, 0, rideId, Instant.EPOCH, "", 0F, 0F, 0F);
    }
}