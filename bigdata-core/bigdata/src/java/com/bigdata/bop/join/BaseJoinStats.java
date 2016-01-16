/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
/*
 * Created on Aug 15, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.counters.CAT;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Extended to expose counters shared by various join operators (some join
 * operators may define more counters as well).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BaseJoinStats extends BOpStats {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public BaseJoinStats() {
    }

    /**
     * The #of duplicate access paths which were detected and filtered out. This
     * will be ZERO (0L) unless the join operator can coalesce access paths.
     */
    public final CAT accessPathDups = new CAT();

    /**
     * The #of access paths which were evaluated. Each access path corresponds
     * to one or more input binding sets having the same binding pattern for the
     * variables appearing in the access path.
     * <p>
     * Note: Some join operators can coalesce duplicate access paths. Such
     * operators may reported fewer access paths here.
     * <p>
     * Note: Hash join operators typically run the access path exactly once (if
     * they are written to use the native heap and their memory budget is
     * exceeded then they may run the access path more than once).
     */
    public final CAT accessPathCount = new CAT();

    /**
     * The running sum of the range counts of the accepted as-bound access
     * paths.
     */
    public final CAT accessPathRangeCount = new CAT();

    /**
     * The #of chunks read from an {@link IAccessPath}.
     */
    public final CAT accessPathChunksIn = new CAT();

    /**
     * The #of elements read from an {@link IAccessPath}.
     */
    public final CAT accessPathUnitsIn = new CAT();

    @Override
    public void add(final BOpStats o) {

        super.add(o);

        if (o instanceof BaseJoinStats) {

            final BaseJoinStats t = (BaseJoinStats) o;

            accessPathDups.add(t.accessPathDups.get());

            accessPathCount.add(t.accessPathCount.get());

            accessPathRangeCount.add(t.accessPathRangeCount.get());

            accessPathChunksIn.add(t.accessPathChunksIn.get());

            accessPathUnitsIn.add(t.accessPathUnitsIn.get());

        }

    }

    @Override
    protected void toString(final StringBuilder sb) {
        super.toString(sb);
        sb.append(",accessPathDups=" + accessPathDups.get());
        sb.append(",accessPathCount=" + accessPathCount.get());
        sb.append(",accessPathRangeCount=" + accessPathRangeCount.get());
        sb.append(",accessPathChunksIn=" + accessPathChunksIn.get());
        sb.append(",accessPathUnitsIn=" + accessPathUnitsIn.get());
    }

}
