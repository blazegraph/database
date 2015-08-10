package com.bigdata.service;

import com.bigdata.service.LoadBalancerService.UpdateTask;

/**
 * Per-host metadata and a score for that host which gets updated
 * periodically by {@link UpdateTask}. {@link HostScore}s are a
 * <em>resource utilization</em> measure. They are higher for a host which
 * is more highly utilized. There are several ways to look at the score,
 * including the {@link #rawScore}, the {@link #rank}, and the
 * {@link #drank normalized double-precision rank}. The ranks move in the
 * same direction as the {@link #rawScore}s - a higher rank indicates
 * higher utilization. The least utilized host is always rank zero (0). The
 * most utilized host is always in the last rank.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HostScore implements Comparable<HostScore> {

    public final String hostname;

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
     * for the host and could not compute its rawScore.
     * 
     * @param hostname
     */
    public HostScore(String hostname) {

        this(hostname, 0d);

    }

    /**
     * Constructor variant used when you have computed the rawStore.
     * 
     * @param hostname
     * @param rawScore
     */
    public HostScore(String hostname, double rawScore) {

        assert hostname != null;

        assert hostname.length() > 0;

        this.hostname = hostname;

        this.rawScore = rawScore;

    }

    public String toString() {

        return "HostScore{hostname=" + hostname + ", rawScore=" + rawScore
                + ", score=" + score + ", rank=" + rank + ", drank=" + drank
                + "}";

    }

    /**
     * Places elements into order by ascending {@link #rawScore} (aka
     * increasing utilization). The {@link #hostname} is used to break any
     * ties.
     */
    public int compareTo(HostScore arg0) {

        if (rawScore < arg0.rawScore) {

            return -1;

        } else if (rawScore > arg0.rawScore) {

            return 1;

        }

        return hostname.compareTo(arg0.hostname);

    }

    /**
     * Normalizes a raw score in the context of totals for some host.
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
