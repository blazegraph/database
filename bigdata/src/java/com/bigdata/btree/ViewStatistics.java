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

import com.bigdata.mdi.IResourceMetadata;
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
    
    public ViewStatistics(final ILocalBTreeView view) {

        // #of sources in the view (very fast).
        int sourceCount = 0, sourceJournalCount = 0, sourceSegmentCount = 0;
        long sumSegBytes = 0L;
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
            for (IResourceMetadata x : pmd.getResources()) {
                sourceCount++;
                if (x.isJournal()) {
                    sourceJournalCount++;
                } else {
                    sourceSegmentCount++;
                    sumSegBytes += x.getFile().length();
                }
            }
            this.sourceCount = sourceCount;
            this.sourceJournalCount = sourceJournalCount;
            this.sourceSegmentCount = sourceSegmentCount;
            this.sumSegBytes = sumSegBytes;
        }

    }
    
}
