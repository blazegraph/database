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

import com.bigdata.counters.AbstractStatisticsCollector;

/**
 * Helper class assigns a raw and a normalized score to each host based on its
 * per-host.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HostScore implements Comparable<HostScore> {

    /** The hostname. */
    private final String hostname;

    /**
     * <code>true</code> iff the host is this host.
     */
    private final boolean thisHost;

    /**
     * The raw score for some host. This is a measure of the load on a host. The
     * measure is computed based on {@link #metrics} using the
     * {@link #scoringRule}.
     */
    private final double rawScore;

    /**
     * The normalized score computed for that host.
     * <p>
     * Note: The {@link #rawScore} is a measure of the <em>load</em> on a host.
     * The normalized {@link #score} is based on
     * <code>1-{@link #rawScore}</code> and is a measure of the normalized
     * amount of capacity to do more work on a host. Load balancing decision are
     * based on this normalized {@link #score}.
     */
    private final double score;

    /**
     * The {@link IHostScoringRule} used to convert the {@link #metrics} into
     * the {@link #rawScore}.
     */
    private final IHostScoringRule scoringRule;
    
    /**
     * The {@link IHostMetrics} associated with this host (this is
     * informational. The {@link #metrics} provide the detailed per-host
     * performance metrics that were intepreted by the {@link #scoringRule} .
     */
    final private IHostMetrics metrics;

//    /** The rank in [0:#scored]. This is an index into the Scores[]. */
//    public int rank = -1;
//
//    /** The normalized double precision rank in [0.0:1.0]. */
//    public double drank = -1d;

    /**
     * Return the raw score for a host, which is a measure of the load on that
     * host. The measure is computed based on {@link IHostMetrics} using some
     * {@link IHostScoringRule}.
     */
    public double getRawScore() {
        return rawScore;
    }

    /**
     * Return the measure of free capacity to do work on the host.
     * <p>
     * Note: The {@link #getRawScore() rawScore} is a measure of the
     * <em>load</em> on a host. The normalized {@link #getScore() score} is
     * based on <code>1-{@link #getRawScore() rawScore}</code> and is a measure
     * of the normalized amount of capacity to do more work on a host. Load
     * balancing decision are based on this normalized {@link #getScore() score}
     * .
     */
    public double getScore() {
        return score;
    }

    /** Return the hostname. */
    public String getHostname() {
        return hostname;
    }

    /**
     * The {@link IHostMetrics} associated with this host (this is
     * informational. The {@link #getMetrics()} provide the detailed per-host
     * performance metrics that were intepreted by the {@link #getScoringRule()}
     * .
     */
    public IHostMetrics getMetrics() {
        return metrics;
    }
    
    /**
     * The {@link IHostScoringRule} used to convert the {@link #getMetrics()}
     * into the {@link #getRawScore()}.
     */
    public IHostScoringRule getScoringRule() {
        return scoringRule;
    }

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
                + ", rawScore=" + rawScore //
                + ", score=" + score //
//                + ", rank=" + rank //
//                + ", drank=" + drank //
                + ", metrics=" + metrics //
                + "}";

    }

    public HostScore(final String hostname, final double rawScore,
            final double totalRawScore, final IHostScoringRule scoringRule,
            final IHostMetrics metrics) {

        if (hostname == null)
            throw new IllegalArgumentException();

        if (hostname.trim().length() == 0)
            throw new IllegalArgumentException();

        this.hostname = hostname;

        this.rawScore = rawScore;

        this.thisHost = AbstractStatisticsCollector.fullyQualifiedHostName
                .equals(hostname);

        this.scoringRule = scoringRule;
        
        this.metrics = metrics;
        
        score = normalize(rawScore, totalRawScore);

    }

    /**
     * Computes the normalized {@link #score} from the {@link #rawScore} in the
     * context of total over the {@link #rawScore}s for some set of hosts.
     * 
     * @param rawScore
     *            The raw score.
     * @param totalRawScore
     *            The raw score computed from the totals.
     * 
     * @return The normalized score.
     */
    static private double normalize(final double rawScore,
            final double totalRawScore) {

        if (totalRawScore == 0d) {

            return 0d;

        }

        return (1d - rawScore) / totalRawScore;

    }

    /**
     * Places elements into order by increasing normalized {@link #getScore()}.
     * The {@link #getHostname()} is used to break any ties (but this does not
     * help when all services are on the same host).
     */
    @Override
    public int compareTo(final HostScore arg0) {

        if (score < arg0.score) {

            return -1;

        } else if (score > arg0.score) {

            return 1;

        }

        return hostname.compareTo(arg0.hostname);

    }

}