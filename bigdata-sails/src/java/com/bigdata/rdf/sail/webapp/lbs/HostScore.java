/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sail.webapp.lbs;

import java.util.Comparator;

import com.bigdata.counters.AbstractStatisticsCollector;

/**
 * Helper class assigns a raw score (load) and a normalized score (normalized
 * load) to each host based on its per-host metrics and a rule for computing the
 * load of a host from those metrics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HostScore {

    /** The hostname. */
    private final String hostname;

    /**
     * <code>true</code> iff the host is this host.
     */
    private final boolean thisHost;

//    /**
//     * The raw score for some host. This is a measure of the load on a host. The
//     * measure is computed based on {@link #metrics} using the
//     * {@link #scoringRule}.
//     */
//    private final double rawScore;

    /**
     * The normalized score for that host.
     * <p>
     * Note: The {@link #rawScore} is a measure of the <em>load</em> on a host.
     * The normalized {@link #score} is a measure of the normalized
     * <em>load</em> on a host. Load balancing decision are based on this
     * normalized {@link #score} (work is assigned to a host in inverse
     * proportion to its normalized {@link #score}).
     */
    private final double score;

//    /**
//     * The {@link IHostScoringRule} used to convert the {@link #metrics} into
//     * the {@link #rawScore}.
//     */
//    private final IHostScoringRule scoringRule;
//    
//    /**
//     * The {@link IHostMetrics} associated with this host (this is
//     * informational. The {@link #metrics} provide the detailed per-host
//     * performance metrics that were intepreted by the {@link #scoringRule} .
//     */
//    final private IHostMetrics metrics;

//    /** The rank in [0:#scored]. This is an index into the Scores[]. */
//    public int rank = -1;
//
//    /** The normalized double precision rank in [0.0:1.0]. */
//    public double drank = -1d;

//    /**
//     * Return the raw score (aka load) for a host. This raw score is an
//     * unnormalized measure of the load on that host. The measure is computed
//     * based on {@link IHostMetrics} using some {@link IHostScoringRule}.
//     */
//    public double getRawScore() {
//        return rawScore;
//    }

    /**
     * Return the normalized load for the host.
     * <p>
     * Note: The {@link #getRawScore() rawScore} is a measure of the
     * <em>load</em> on a host. The normalized {@link #getScore() score} is the
     * normalized load for a host. Load balancing decision are based on this
     * normalized {@link #getScore() score} (work is assigned to hosts in
     * inverse proportion to the normalized load of the host).
     */
    public double getScore() {
        return score;
    }

    /** Return the hostname. */
    public String getHostname() {
        return hostname;
    }

//    /**
//     * The {@link IHostMetrics} associated with this host (optional). The
//     * {@link #getMetrics()} provide the detailed per-host performance metrics
//     * that were intepreted by the {@link #getScoringRule()} .
//     */
//    public IHostMetrics getMetrics() {
//        return metrics;
//    }
//    
//    /**
//     * The {@link IHostScoringRule} used to convert the {@link #getMetrics()}
//     * into the {@link #getRawScore()} (optional).
//     */
//    public IHostScoringRule getScoringRule() {
//        return scoringRule;
//    }

    /**
     * Return <code>true</code> iff the host is this host.
     */
    public boolean isThisHost() {
        return thisHost;
    }

    @Override
    public String toString() {

        return "HostScore"//
                + "{hostname=" + hostname //
                + ", thisHost=" + thisHost//
//                + ", rawScore=" + rawScore //
                + ", score=" + score //
//                + ", rank=" + rank //
//                + ", drank=" + drank //
//                + ", metrics=" + metrics //
//                + ", scoringRule=" + scoringRule //
                + "}";

    }

    /**
     * 
     * @param hostname
     *            The hostname (required, must be non-empty).
     *            @param score The normalized availability score for this
     *            host.
//     * @param rawScore
//     *            The unnormalized load for that host.
//     * @param totalRawScore
//     *            The total unnormalized load across all hosts.
     */
//    * @param metrics
//    *            The performance metrics used to compute the unnormalized load
//    *            for each host (optional).
//    * @param scoringRule
//    *            The rule used to score those metrics (optional).
    public HostScore(//
            final String hostname,//
            final double score
//            final double rawScore,//
//            final double totalRawScore //
//            final IHostMetrics metrics,//
//            final IHostScoringRule scoringRule//
    ) {

        if (hostname == null)
            throw new IllegalArgumentException();

        if (hostname.trim().length() == 0)
            throw new IllegalArgumentException();

        if (score < 0d || score > 1d)
            throw new IllegalArgumentException();
        
        this.hostname = hostname;

        this.score = score;
        
//        this.rawScore = rawScore;

        this.thisHost = AbstractStatisticsCollector.fullyQualifiedHostName
                .equals(hostname);

//        this.scoringRule = scoringRule;
//        
//        this.metrics = metrics;
        
//        score = normalize(rawScore, totalRawScore);
        
    }

    /**
     * Places elements into order by decreasing {@link #getScore() normalized
     * load}. The {@link #getHostname()} is used to break any ties.
     */
    public final static Comparator<HostScore> COMPARE_BY_HOSTNAME = new Comparator<HostScore>() {

        @Override
        public int compare(final HostScore t1, final HostScore t2) {

            if (t1.score < t2.score) {

                return 1;

            } else if (t1.score > t2.score) {

                return -1;

            }

            return t1.hostname.compareTo(t2.hostname);

        }

    };

    /**
     * Orders by hostname. This provides a stable way of viewing the data in
     * <code>/bigdata/status</code>.
     */
    public final Comparator<HostScore> COMPARE_BY_SCORE = new Comparator<HostScore>() {

        @Override
        public int compare(final HostScore t1, final HostScore t2) {

            return t1.hostname.compareTo(t2.hostname);

        }
        
    };

}