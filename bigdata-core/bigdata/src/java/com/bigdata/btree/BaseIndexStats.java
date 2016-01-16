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
package com.bigdata.btree;

import java.io.PrintWriter;
import java.util.Map;


/**
 * Basic stats that are available for all index types and whose collection does
 * not require visitation of the index pages.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BaseIndexStats {

    /** The type of index. */
    public IndexTypeEnum indexType;
    /**
     * The name associated with the index -or- <code>null</code> if the index is
     * not named.
     */
    public String name;
    /**
     * The current branching factor for the index.
     * 
     * TODO GIST: [m] is BTree specific. The [addressBits] concept is the
     * parallel for the HTree. This field should probably be moved into the
     * concrete instances of the {@link PageStats} class.
     */
    public int m;
    /** The #of entries in the index. */
    public long ntuples;
    /** The height (aka depth) of the index */
    public int height;
    /** The #of nodes visited. */
    public long nnodes;
    /** The #of leaves visited. */
    public long nleaves;

    /**
     * Zero-arg constructor does NOT initialize the fields.
     */
    public BaseIndexStats() {

    }

    /**
     * Initializes the fields for the specified index.
     */
    public BaseIndexStats(final ICheckpointProtocol ndx) {

        if (ndx == null)
            throw new IllegalArgumentException();

        final ICheckpoint checkpoint = ndx.getCheckpoint();

        final IndexMetadata metadata = ndx.getIndexMetadata();

        this.indexType = checkpoint.getIndexType();

        this.name = metadata.getName();

        switch (indexType) {
        case BTree:
            this.m = metadata.getBranchingFactor();
            break;
        case HTree:
            m = ((HTreeIndexMetadata) metadata).getAddressBits();
            break;
        case Stream:
            m = 0; // N/A
            break;
        default:
            throw new AssertionError("Unknown indexType=" + indexType);
        }
        
        /*
         * Note: The "height" of the HTree must be computed dynamically since
         * the HTree is not a balanced tree. It will be reported as ZERO (0)
         * using this logic.
         */
        this.height = checkpoint.getHeight();

        this.ntuples = checkpoint.getEntryCount();
        
        this.nnodes = checkpoint.getNodeCount();
        
        this.nleaves = checkpoint.getLeafCount();
        
    }

    /**
     * Return the header row for a table.
     * 
     * @return The header row.
     */
    public String getHeaderRow() {

        final StringBuilder sb = new StringBuilder();

        sb.append("name");
        sb.append('\t');
        sb.append("indexType");
        sb.append('\t');
        sb.append("m");
        sb.append('\t');
        sb.append("height");
        sb.append('\t');
        sb.append("nnodes");
        sb.append('\t');
        sb.append("nleaves");
        sb.append('\t');
        sb.append("nentries");

        return sb.toString();
    }

    /**
     * Return a row of data for an index as aggregated by this {@link PageStats}
     * object.
     * 
     * @see #getHeaderRow()
     */
    public String getDataRow() {

        final BaseIndexStats stats = this;

        final StringBuilder sb = new StringBuilder();

        sb.append(name);
        sb.append('\t');
        sb.append(indexType);
        sb.append('\t');
        sb.append(stats.m);
        sb.append('\t');
        sb.append(stats.height);
        sb.append('\t');
        sb.append(stats.nnodes);
        sb.append('\t');
        sb.append(stats.nleaves);
        sb.append('\t');
        sb.append(stats.ntuples);

        return sb.toString();
    }


    /**
     * Helper method may be used to write out a tab-delimited table of the
     * statistics.
     * 
     * @param out
     *           Where to write the statistics.
     */
    static public void writeOn(final PrintWriter out,
          final Map<String, BaseIndexStats> statsMap) {

       /*
        * Write out the header.
        */
       boolean first = true;

       for (Map.Entry<String, BaseIndexStats> e : statsMap.entrySet()) {

          final String name = e.getKey();

          final BaseIndexStats stats = e.getValue();

          if (stats == null) {

             /*
              * Something for which we did not extract the PageStats.
              */

             out.println("name: " + name + " :: no statistics?");

             continue;

          }

          if (first) {

             out.println(stats.getHeaderRow());

             first = false;

          }

          /*
           * Write out the stats for this index.
           */

          out.println(stats.getDataRow());

       }

    }

}
