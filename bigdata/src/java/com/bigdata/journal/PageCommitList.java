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
 * Created on Oct 31, 2006
 */

package com.bigdata.journal;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * <p>
 * A list of dirty pages to be flushed on commit. The page commit list defers
 * actual writes on the backing file while dirty pages are accumulated. Once the
 * page list is full, and in any event when the transaction commits, the dirty
 * pages are written onto the backing file.
 * </p>
 * <p>
 * The journal only uses sequential writes for slot data. This makes several
 * optimizations possible. First, we know that we will never touch a page
 * already on the list once a new page is added to the list (the rare exception
 * is when a nearly full journal wraps around, and that could trigger an eager
 * write if necessary). Second, we know that the writes will be ordered based on
 * the order of the pages in the list (again, the exception is when the journal
 * wraps around).
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Finish this implementation and integrate with the various
 *       {@link DirectBufferStrategy} and the {@link DiskOnlyStrategy} - those
 *       are the only two strategies that can utilize a page buffer.
 * 
 * @todo Measure the impact of the page-based commit list with
 *       {@link BenchmarkJournalWriteRate}. Examine the impact of "forceWrites"
 *       in this context.
 */
public class PageCommitList {

    /**
     * The maximum #of pages to be buffered.
     */
    private final int npages;
    /**
     * The size of a "page".
     */
    private final int pageSize;
    /**
     * The backing file.
     */
    private final RandomAccessFile raf;
    /**
     * An array of {@link PageView}s organized as a ring buffer.
     */
    private final PageView[] dirtyList;
    /**
     * The index of the current entry in the list. When position == limit, then
     * the list is empty.
     */
    private int position = 0;
    /**
     * The index of the first clean entry in the list.
     */
    private int limit = 0;
    
    /**
     * @param npages
     *            The #of pages to buffer.
     * @param pageSize
     *            The size of a page.
     * @param raf
     *            The random access file to which pages will be evicted.
     * 
     * @todo Provide for efficient installation reads when the first write
     *       occurs on a page, thereby adding it to the commit list, or no later
     *       than when we will write out the page (unless only a contiguous
     *       region of the page is dirty, in which case we can just write out
     *       that region).
     */
    public PageCommitList(int npages,int pageSize,RandomAccessFile raf) {
        
        assert npages > 0;
        assert pageSize > 0;
        assert raf != null;

        this.npages = npages;
        this.pageSize = pageSize;
        this.raf = raf;

        dirtyList = new PageView[ npages ];
        
        for( int i=0; i<npages; i++ ) {
            
            dirtyList[ i ] = new PageView(pageSize);
            
        }
        
    }

    /**
     * A view of a page together with metadata defining the dirty span of the
     * page.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Add the page offset in the file and a method to allocate a pageView
     *       for a given offset. The dirtyList then forms both a pool of buffers
     *       and the set of dirty buffers to be written (and the order in which
     *       they will be written) while the offset on each pageView specifies
     *       where it will be written on the backing file. When the offsets of
     *       dirty pages are contiguous and the pages are either entirely dirty
     *       or we have an installation read so the page image is entirely
     *       correct, then we can write multiple pages in a single IO request on
     *       the file.
     */
    static class PageView {
        
        final ByteBuffer buf;
        
        boolean dirty = false;
        
        /**
         * The position of the first dirty byte in the buffer.
         */
        int position = 0;
        
        /**
         * The limit of the dirty span of the buffer (the index of the first
         * non-dirty byte).
         */
        int limit = 0;
        
        PageView(int pageSize) {
            
            assert pageSize > 0;
            
            buf = ByteBuffer.allocateDirect(pageSize);
            
        }
        
    }
    
}
