/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Dec 5, 2006
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.bigdata.journal.IRawStore;

/**
 * Class supports a post-order construction of a "perfect" b+tree given sorted
 * records. There are two main use cases:
 * <ol>
 * <li>Evicting a key range of an index into an optimized on-disk index. In
 * this case, the input is a btree that is ideally backed by a fully buffered
 * {@link IRawStore} so that no random reads are required.</li>
 * <li>Merging index segments. In this case, the input is typically records
 * emerging from a merge-sort. There are two distinct cases here. In one, we
 * simply have raw records that are being merged into an index. This might occur
 * when merging two key ranges or when external data are being loaded. In the
 * other case we are processing two timestamped versions of an overlapping key
 * range. In this case, the more recent version may have "delete" markers
 * indicating that a key present in an older version has been deleted in the
 * newer version. Also, key-value entries in the newer version replaced (rather
 * than are merged with) key-value entries in the older version. If an entry
 * history policy is defined, then it must be applied here to cause key-value
 * whose retention is no longer required by that policy to be dropped.</li>
 * </ol>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see "Post-order B-Tree Construction" by Lawerence West, ACM 1992.
 * 
 * FIXME Implement the post-order builder. Support each of the use cases,
 * including external sorting.
 */
public class PostOrderBuilder {

    /**
     * @todo consider the file mode and buffering. We should at least buffer
     *       several pages of data per write and can experiment with writing
     *       through (vs caching writes in the OS layer). The file does not need
     *       to be "live" until it is completely written, so there is no need to
     *       update file metadata until the end of the build process.
     */
    final String mode = "rws";
    
    final RandomAccessFile raf;

    final IRangeIterator itr;
    
    public PostOrderBuilder(File out, BTree btree, Object fromKey, Object toKey)
            throws IOException {

        raf = new RandomAccessFile(out,mode);

        itr = btree.rangeIterator(fromKey,toKey);
        
    }

    /**
     * Choose the height and branching factor (aka order) of the generated tree.
     * This choice is made by choosing a height and order for the tree such
     * that:
     * 
     * <pre>
     *   2(d + 1)&circ;(h-l) - 1 &lt;= N &lt;= (2d + 1)&circ;h - l
     * </pre>
     * 
     * where
     * <ul>
     * <li>d := the minimum #of keys in a node of the generated tree (m/2).</li>
     * <li>h := the height of the generated tree (origin one (1)).</li>
     * <li>N := the #of entries (rows of data)</li>
     * </ul>
     * 
     * This can be restated as:
     * 
     * <pre>
     *  
     *   2(m/2 + 1)&circ;h - 1 &lt;= N &lt;= (m + 1)&circ;(h+1) - l
     * </pre>
     * 
     * where
     * <ul>
     * <li>m := the branching factor of the generated tree.</li>
     * <li>h := the height of the generated tree (origin zero(0)).</li>
     * <li>N := the #of entries (rows of data)</li>
     * </ul>
     * 
     * @todo The #of entries to be placed into the generated perfect index must
     *       be unchanging during this process. This suggests that it is best to
     *       freeze the journal, opening a new journal for continued writes, and
     *       then evict all index ranges in the frozen journal into perfect
     *       indices.
     * 
     * @todo Note that this routine is limited to an index subrange with no more
     *       entries than can be represented in an int32 signed value.
     */
    protected void phase1(int nentries) {

        /*
         * @todo solve for the desired height, where h is the #of non-leaf nodes
         * and is zero (0) if the tree consists of only a root leaf.  We want to
         * minimize the height as long as the node/leaf size is not too great.
         */
        int h = 0;
        
        /*
         * @todo solve for the desired branching factor (#of children for a node
         * or the #of values for a leaf. The #of keys for a node is m-1. The
         * branching factor has to be bounded since there is some branching
         * factor at which any btree fits into the root leaf. Therefore an
         * allowable upper range for m should be an input, e.g., m = 4096. This
         * can be choosen with an eye to the size of a leaf on the disk since we
         * plan to have the index nodes in memory but to read the leaves from
         * disk. Since the size of a leaf varies by the key and value types this
         * can either be a SWAG, e.g., 1024 or 4096, or it can be computed based
         * on the actual average size of the leaves as written onto the store.
         * 
         * Note that leaves in the journal index ranges will typically be much
         * smaller since the journal uses smaller branching factors to minimize
         * the cost of insert and delete operations on an index.
         * 
         * In order to minimize IO we probably want to write the leaves onto the
         * output file as we go (blocking them in a page buffer of at least 32K)
         * and generate the index nodes in a big old buffer (since we do not
         * know their serialized size in advance) and then write them out all at
         * once.
         */
        int m = 4;
        
        /*
         * The minimum #of entries that will fit in a btree given (m,h).
         */
        int min = (int) Math.pow(m+1, h)-1;
        
        /*
         * The maximum #of entries that will fit in a btree given (m,h).
         */
        int max = (int) Math.pow(m, h+1);
        
        if( min <= nentries && nentries<= max ) {
            
            /*
             * A btree may be constructed for this many entries with height := h
             * and branching factor := m. There will be many such solutions and
             * one needs to be choosen before building the tree.
             * 
             * To build the tree with the fewest possible nodes, select the
             * combination of (m,h) with the smallest value of h. This is what
             * we are looking for since there will be no further inserted into
             * the generated index (it is read-only for our purposes). Other
             * applications might want to "dial-in" some sparseness to the index
             * by choosing a larger value of h so that more inserts could be
             * absorbed without causing nodes split.  This is not an issue for
             * us since we never insert into the generated index file.
             */
            
        }

        /*
         * compute per-level values given (h,m).
         * 
         * Note: our h is h-1 for West (we count the root as h == 0 and West
         * counts it as h == 1).  While West defines the height in terms of
         * the #of nodes in a path to a leaf from the root, West is working
         * with a b-tree and there is no distinction between nodes and leaves.
         */
        int r[] = new int[h-1];
        int n[] = new int[h-1];
        
    }

    /**
     *
     */
    protected void phase2() {

    }

}
