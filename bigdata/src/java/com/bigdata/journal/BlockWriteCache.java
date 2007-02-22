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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <p>
 * A block-oriented write cache used by the {@link BufferMode#Direct} and
 * {@link BufferMode#Disk} modes. This buffer is designed to recapture an
 * approximate 5x penalty observed with per-slot IOs when compared to raw
 * per-block IOs.
 * </p>
 * <p>
 * The write buffer establishes an array of pre-allocated direct buffers, each
 * sized to a "block". The buffers are arranged logically into a ring of
 * buffers. Data are written through onto the buffers in sequence. If all
 * buffers are full, then the buffers are flushed using a single nio operation -
 * {@link FileChannel#write(java.nio.ByteBuffer[], int, int)}. When the journal
 * is to be forced to disk, all dirty buffers MUST be written to disk BEFORE the
 * channel is forced to disk.
 * </p>
 * <p>
 * The write buffers MUST be initialized from the journal's current state. With
 * the "direct" mode, this can be a simple copy of the relevant region of the
 * direct buffer. With the disk-only more, this means that we have to read the
 * page from the disk before writing on it. The "direct" mode only reads from
 * the direct buffer, so the page cache will not introduce any incoherence and
 * the {@link DirectBufferStrategy} does NOT need to read through this buffer.
 * However, the "disk" mode normally reads from the disk. Therefor the
 * {@link DiskOnlyStrategy} MUST read from this buffer in preference to disk in
 * order to be coherent.
 * </p>
 * 
 * @todo Implement and integrate this buffering strategy in order to recover an
 * approximately 5x overhead introduced by doing too many IOs.
 * 
 * @todo The write strategy can (nearly) rely that writes are always on
 *       increasing positions, except when the buffer wraps. This means that we
 *       can simply write all buffers to disk. For the direct mode, we could
 *       immediately release the buffers once they are written to disk since we
 *       never read from a buffer. For the "disk" mode, we could release the
 *       buffers on demand (e.g., LRU) and just mark them clean when they are
 *       written to disk. <br>
 *       The above would also give us a LRU block buffer for the "disk-only"
 *       strategy, which seems like a good place to start. Note that a read
 *       buffer is NOT required for any other mode since they either are already
 *       fully buffered or memory-mapped. A read cache is not super critical
 *       since we can use a direct mode unless resources are scarce, but it will
 *       improve fail soft as resources become tighter. At this time I do not
 *       have the benchmarks developed that would be required to tune a read
 *       buffer at this time. The read buffer needs to serve data migration (an
 *       LRU certainly makes sense there, and suggests migrating committed data
 *       in reverse write order to maximize page hits). It needs to serve random
 *       reads against committed state that has not been migrated to the
 *       read-optimized database. It needs to serve random reads against
 *       historical committed state that has been replaced on the read-optimized
 *       database, but which is still retained on the journal since there are
 *       active transactions that can read that data. Finally, It needs to serve
 *       logical deletion on index nodes - for this last case an index node
 *       cache would seem to be all that is required (and it will be required in
 *       any case).
 * 
 * @todo A possible tweak on the write strategy would track the min and max
 *       position on each buffer and write no more than that slice of the buffer
 *       by tweaking its position and limit before the nio call. This would
 *       limit the amount of IO, but not change the #of IOs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BenchmarkJournalWriteRate
 * 
 * @deprecated never implemented and to be replaced by aio support in {@link DiskOnlyStrategy}
 */
public class BlockWriteCache {

    final int nbuffers;
    final int bufferSize;
    final ByteBuffer[] buffers;
    final FileChannel channel;

    /**
     * 
     * @param nbuffers
     *            The #of buffers to allocate.
     * @param bufferSize
     *            The size of each buffer (8k+ recommended, 1k minimum).
     * @param channel
     *            The channel onto which to write dirty buffers.
     */
    public BlockWriteCache(int nbuffers,int bufferSize,FileChannel channel) {
        
        assert nbuffers > 0 ;
        
        assert bufferSize > 1024;
        
        assert channel != null;
        
        this.nbuffers = nbuffers;
        
        this.bufferSize = bufferSize;
        
        this.channel = channel;
        
        this.buffers = new ByteBuffer[ nbuffers ];
        
        for( int i=0; i<nbuffers; i++ ) {
            
            buffers[ i ] = ByteBuffer.allocateDirect(bufferSize);
            
        }
        
    }

    /**
     * Writes all dirty buffers onto the channel.
     *  
     * @throws IOException
     */
    public void force() throws IOException {
        
        channel.write(buffers);
        
    }
    
    /*
     * @todo Choose the appropriate buffer and write through onto it at the
     * appropriate position.  This requires that we know which "block" each
     * buffer is presently covering, and also whether or not the buffer is
     * currently dirty.  If the block is not buffered, then we need to read
     * it.  For the "direct" mode we want to copy the data from the direct
     * buffer.  For the "disk" mode we want to transfer the data from the
     * channel.  If there are no buffers free, then we need to write the
     * existing buffers to disk.
     */
    public void write(ByteBuffer src,long pos) {

        throw new UnsupportedOperationException();

    }

    /*
     * @todo must indicate whether the data was available in a buffer, or just
     * read through to disk when it was not available.  If the latter, then this
     * might eventually stack over a block read cache.
     */
    public boolean read(ByteBuffer dst,long pos) {
        
        throw new UnsupportedOperationException();
        
    }
    
}
