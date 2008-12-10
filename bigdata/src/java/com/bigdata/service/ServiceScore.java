package com.bigdata.service;

import java.util.UUID;

import com.bigdata.service.LoadBalancerService.UpdateTask;

/**
 * Per-service metadata and a score for that service which gets updated
 * periodically by the {@link UpdateTask}. {@link ServiceScore}s are a
 * <em>resource utilization</em> measure. They are higher for a service
 * which is more highly utilized. There are several ways to look at the
 * score, including the {@link #rawScore}, the {@link #rank}, and the
 * {@link #drank normalized double-precision rank}. The ranks move in the
 * same direction as the {@link #rawScore}s - a higher rank indicates
 * higher utilization. The least utilized service is always rank zero (0).
 * The most utilized service is always in the last rank.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceScore implements Comparable<ServiceScore> {

    public final String hostname;

    public final UUID serviceUUID;

    public final String serviceName;

    /** The raw score computed for that service. */
    public final double rawScore;

    /** The normalized score computed for that service. */
    public double score;

    /** The rank in [0:#scored].  This is an index into the Scores[]. */
    public int rank = -1;

    /** The normalized double precision rank in [0.0:1.0]. */
    public double drank = -1d;

    /**
     * Constructor variant used when you do not have performance counters
     * for the service and could not compute its rawScore.
     * 
     * @param hostname
     * @param serviceUUID
     */
    public ServiceScore(String hostname, UUID serviceUUID, String serviceName) {

        this(hostname, serviceUUID, serviceName, 0d);

    }

    /**
     * Constructor variant used when you have computed the rawStore.
     * 
     * @param hostname
     * @param serviceUUID
     * @param rawScore
     */
    public ServiceScore(String hostname, UUID serviceUUID, String serviceName,
            double rawScore) {

        if (hostname == null)
            throw new IllegalArgumentException();

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (serviceName == null)
            throw new IllegalArgumentException();

        this.hostname = hostname;

        this.serviceUUID = serviceUUID;

        this.serviceName = serviceName;

        this.rawScore = rawScore;

    }

    public String toString() {

        return "ServiceScore{hostname=" + hostname + ", serviceUUID="
                + serviceUUID + ", serviceName="
                + (serviceName == null ? "N/A" : serviceName) + ", rawScore="
                + rawScore + ", score=" + score + ", rank=" + rank + ", drank="
                + drank + "}";

    }

    /**
     * Places elements into order by ascending {@link #rawScore}. The
     * {@link #serviceUUID} is used to break any ties.
     */
    public int compareTo(ServiceScore arg0) {

        if (rawScore < arg0.rawScore) {

            return -1;

        } else if (rawScore > arg0.rawScore) {

            return 1;

        }

        return serviceUUID.compareTo(arg0.serviceUUID);

    }

    /**
     * Normalizes a raw score in the context of totals for some service.
     * 
     * @param rawScore
     *            The raw score.
     * @param totalRawScore
     *            The raw score computed from the totals.
     *            
     * @return The normalized score.
     */
    static public double normalize(double rawScore, double totalRawScore) {

        if (totalRawScore == 0d) {

            return 0d;

        }

        return rawScore / totalRawScore;

    }

}
