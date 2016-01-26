/*

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
package com.bigdata.service;

import com.bigdata.mdi.IPartitionMetadata;

/**
 * Describes a "split" of keys for a batch operation. This is used in scale-out
 * where the operation is parallelized across multiple index partitions. It is
 * also used within a single index partition when an operation that has a lot of
 * keys in its keys[] is parallelized over sub-key-ranges of that keys[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Split {

    /**
     * The index partition that spans the keys in this split.
     */
    public final IPartitionMetadata pmd;

    /**
     * Index of the first key in this split.
     */
    public final int fromIndex;

    /**
     * Index of the first key NOT included in this split.
     */
    public final int toIndex;

    /**
     * The #of keys in this split (toIndex - fromIndex).
     */
    public final int ntuples;

    /**
     * Create a representation of a split point without specifying the from/to
     * tuple index.
     * 
     * @param pmd
     *            The metadata for the index partition within which the keys in
     *            this split lie.
     */
    public Split(final IPartitionMetadata pmd) {

        this(pmd, 0/* fromIndexIgnored */, 0/* toIndexIgnored */);

    }
    
    /**
     * Create a representation of a split point.
     * 
     * @param pmd
     *            The metadata for the index partition within which the keys in
     *            this split lie (optional and used only in scale-out).
     * @param fromIndex
     *            The index of the first key that will enter that index
     *            partition (inclusive lower bound).
     * @param toIndex
     *            The index of the first key that will NOT enter that index
     *            partition (exclusive upper bound).
     */
    public Split(final IPartitionMetadata pmd, final int fromIndex,
            final int toIndex) {

//        assert pmd != null;

        this.pmd = pmd;

        if (fromIndex < 0)
            throw new IllegalArgumentException("fromIndex=" + fromIndex);

        if (toIndex < fromIndex)
            throw new IllegalArgumentException("fromIndex=" + fromIndex
                    + ", toIndex=" + toIndex);

        this.fromIndex = fromIndex;

        this.toIndex = toIndex;

        this.ntuples = toIndex - fromIndex; // @todo this is off by one? (to - from + 1)?

    }

    /**
	 * Hash code is based on the {@link IPartitionMetadata} hash code if given
	 * (if is always present for scale-out client requests) and otherwise
	 * {@link #fromIndex} (for example, when parallelizing operations within a
	 * single index per BLGZ-1537).
	 * <p>
	 * Note: The historical hash code was just based on the
	 * {@link IPartitionMetadata} and would thrown out an NPE if there was no
	 * {@link IPartitionMetadata}. This was changed as part of BLZG-1537 to
	 * support cases where the {@link IPartitionMetadata} was not given (for
	 * index local parallelism).
	 * 
	 * @see BLZG-1537 (Schedule more IOs when loading data)
	 */
    @Override
    public int hashCode() {

		if (pmd != null)
			return pmd.hashCode();

		return fromIndex;

    }

    @Override
	public boolean equals(final Object o) {
		
		if (o == this)
			return true;
		
		if (o instanceof Split)
			return equals((Split) o);
	
		return false;
	}

	public boolean equals(final Split o) {

        if (fromIndex != o.fromIndex)
            return false;

        if (toIndex != o.toIndex)
            return false;

        if (ntuples != o.ntuples)
            return false;

        if (!pmd.equals(o.pmd))
            return false;

        return true;

    }

    /**
     * Human friendly representation.
     */
    public String toString() {
        
        return "Split"+
        "{ ntuples="+ntuples+
        ", fromIndex="+fromIndex+
        ", toIndex="+toIndex+
        ", pmd="+pmd+
        "}";
        
    }
    
}
