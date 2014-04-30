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
    final public String hostname;

    /**
     * <code>true</code> iff the host is this host.
     */
    final public boolean thisHost;

    /** The raw (write) score computed for that index partition. */
    private final double rawScore;

    /** The normalized score computed for that index partition. */
    final public double score;

    /** The rank in [0:#scored]. This is an index into the Scores[]. */
    public int rank = -1;

    /** The normalized double precision rank in [0.0:1.0]. */
    public double drank = -1d;

    @Override
    public String toString() {

        return "HostScore{hostname=" + hostname + ", thisHost=" + thisHost
                + ", rawScore=" + rawScore + ", score=" + score + ", rank="
                + rank + ", drank=" + drank + "}";

    }

    public HostScore(final String hostname, final double rawScore,
            final double totalRawScore) {

        if (hostname == null)
            throw new IllegalArgumentException();

        if (hostname.trim().length() == 0)
            throw new IllegalArgumentException();

        this.hostname = hostname;

        this.rawScore = rawScore;

        this.thisHost = AbstractStatisticsCollector.fullyQualifiedHostName
                .equals(hostname);

        score = normalize(rawScore, totalRawScore);

    }

    /**
     * Normalizes a raw score in the context of totals for some data
     * service.
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

        return rawScore / totalRawScore;

    }

    /**
     * Places elements into order by ascending {@link #rawScore}. The
     * {@link #hostname} is used to break any ties.
     */
    @Override
    public int compareTo(final HostScore arg0) {

        if (rawScore < arg0.rawScore) {

            return -1;

        } else if (rawScore > arg0.rawScore) {

            return 1;

        }

        return hostname.compareTo(arg0.hostname);

    }

}