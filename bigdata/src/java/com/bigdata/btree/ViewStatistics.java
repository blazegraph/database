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
 * Created on Jan 18, 2010
 */

package com.bigdata.btree;

import com.bigdata.mdi.LocalPartitionMetadata;

/**
 * Helper class collects some statistics about an {@link ILocalBTreeView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ViewStatistics {

    /**
     * The #of journals and index segments in the view.
     */
    public final int sourceCount;
    
    /**
     * The #of journals in the view.
     */
    public final int sourceJournalCount;

    /**
     * The #of index segments in the view.
     */
    public final int sourceSegmentCount;

    /**
     * The sum of the size on disk across the index segments in the view.
     */
    public final long sumSegBytes;

    /**
     * Collects some statistics about the view.
     * <p>
     * Note: If the view is assembled solely from the {@link BTree} on a journal
     * rather than from all sources in its {@link LocalPartitionMetadata} then
     * statistics about any {@link IndexSegment}s in the view WILL NOT be
     * reported.
     * 
     * @param view
     *            The view.
     */
    public ViewStatistics(final ILocalBTreeView view) {

        // #of sources in the view (very fast).
        final LocalPartitionMetadata pmd = view.getIndexMetadata().getPartitionMetadata();
        if (pmd == null) {
            /*
             * If this is not a view comprised of multiple sources then figure
             * out whether the backing store is an index segment or not.
             */
            final boolean isSeg = view.getMutableBTree().getStore() instanceof IndexSegmentStore;
            this.sourceCount = 1;
            this.sourceJournalCount = isSeg ? 0 : 1;
            this.sourceSegmentCount = isSeg ? 1 : 0;
            this.sumSegBytes = isSeg ? (view.getMutableBTree().getStore()
                    .size()) : 0;
        } else {
            /*
             * This is a view comprised (at least logically) of multiple source
             * B+Tree components.
             */
            final AbstractBTree[] sources = view.getSources();
//            final IResourceMetadata[] resources = pmd.getResources();
//            if (sources.length != resources.length) {
//                /*
//                 * The probable cause here is passing in a BTree object from the
//                 * live journal rather than the FusedView assembled from the
//                 * live journal and some historical journals or index segments.
//                 */
//                throw new IllegalStateException();
//            }
            int sourceCount = 0, sourceJournalCount = 0, sourceSegmentCount = 0;
            long sumSegBytes = 0L;
//            for (IResourceMetadata x : resources) {
            for(AbstractBTree btree : sources) {
                if (btree instanceof IndexSegment) {
                    sourceSegmentCount++;
                    /*
                     * Note: This uses the index segment size as self-reported
                     * by the segStore since the IResourceMetadata#getFile()
                     * object can not be safely resolved to the backing file in
                     * the file system (it is not an absolute file path).
                     */
//                    if (sourceCount >= sources.length) {
//                        throw new AssertionError("sourceCount=" + sourceCount
//                                + ", sources=" + Arrays.toString(sources)
//                                + ", pmd.getResources()="
//                                + Arrays.toString(pmd.getResources()));
//                    }
                    sumSegBytes += sources[sourceCount].getStore().size();
                } else {
                    sourceJournalCount++;
                }                
                sourceCount++;
            }
            this.sourceCount = sourceCount;
            this.sourceJournalCount = sourceJournalCount;
            this.sourceSegmentCount = sourceSegmentCount;
            this.sumSegBytes = sumSegBytes;
        }

    }
    
}
