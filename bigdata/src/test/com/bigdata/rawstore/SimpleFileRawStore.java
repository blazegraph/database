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
/*
 * Created on Jan 31, 2007
 */

package com.bigdata.rawstore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.ResourceState;


/**
 * A simple persistent unbuffered implementation backed by a file.
 * 
 * @see {@link TemporaryRawStore}, which provides a solution for temporary data
 *      that begins with the benefits of a memory-resident buffer and then
 *      converts to a disk-based store on overflow.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleFileRawStore extends AbstractRawWormStore {

    private boolean open = true;
    
    public final File file;
    protected final RandomAccessFile raf;
    
//    /**
//     * This provides a purely transient means to identify deleted records.  This
//     * data does NOT survive restart of the store. 
//     */
//    private final Set<Long> deleted = new HashSet<Long>();

    /**
     * Open a store. The file will be created if it does not exist and it is
     * opened for writing. If the file is opened for writing, then an exception
     * will be thrown unless an exclusive lock can be obtained on the file.
     * 
     * @param file
     *            The name of the file to use as the backing store.
     * @param mode
     *            The file open mode for
     *            {@link RandomAccessFile#RandomAccessFile(File, String)()}.
     */
    public SimpleFileRawStore(File file, String mode) throws IOException {
        
        super(WormAddressManager.DEFAULT_OFFSET_BITS);
        
        if (file == null)
            throw new IllegalArgumentException("file is null");
        
        this.file = file;
        
        raf = new RandomAccessFile(file,mode);

        if( mode.indexOf("w") != -1 ) {

            if (raf.getChannel().tryLock() == null) {

                throw new IOException("Could not lock file: "
                        + file.getAbsoluteFile());

            }
            
        }
        
    }
    
    public boolean isOpen() {

        return open;
        
    }
    
    public boolean isStable() {
        
        return true;
        
    }

    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    public File getFile() {
        
        return file;
        
    }
    
    public IResourceMetadata getResourceMetadata() {
        
        return new IResourceMetadata() {

            public boolean equals(IResourceMetadata o) {
             
                return this == o;
                
            }

            public long getCreateTime() {
                
                // does not support commit
                return 0L;
                
            }

            public String getFile() {

                // no backing file.
                return file.toString();
                
            }

            public UUID getUUID() {

                // no UUID.
                return null;
                
            }

            public boolean isIndexSegment() {

                // not index segment.
                return false;
                
            }

            public boolean isJournal() {
                
                // not journal.
                return false;
                
            }

            public long size() {

                // #of bytes not available.
                return 0L;
                
            }

            public ResourceState state() {

                // presumed live.
                return ResourceState.Live;
                
            }
            
        };
        
    }

    /**
     * This also releases the lock if any obtained by the constructor.
     */
    public void close() {
        
        if( !open ) throw new IllegalStateException();

        open = false;

        try {

            raf.close();
            
        } catch(IOException ex) { 
            
            throw new RuntimeException(ex);
            
        }
        
    }

    public void delete() {
        
        if(open) throw new IllegalStateException();
        
        if(!file.delete()) {
            
            throw new RuntimeException("Could not delete: "
                    + file.getAbsolutePath());
            
        }

    }
    
    public void closeAndDelete() {
        
        close();

        delete();
        
    }

    public ByteBuffer read(long addr) {

        if (addr == 0L)
            throw new IllegalArgumentException("Address is 0L");

        final long offset = getOffset(addr);

        final int nbytes = getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(
                    "Address encodes record length of zero");

        }

//        if (deleted.contains(addr)) {
//
//            throw new IllegalArgumentException(
//                    "Address was deleted in this session");
//
//        }

        try {

            if (offset + nbytes > raf.length()) {

                throw new IllegalArgumentException("Address never written.");

            }

            // allocate a new buffer of the exact capacity.

            ByteBuffer dst = ByteBuffer.allocate(nbytes);

            // copy the data into the buffer.

            raf.getChannel().read(dst, offset);

            // flip for reading.

            dst.flip();

            // return the buffer.

            return dst;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }

    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException("Buffer is null");

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException("No bytes remaining in buffer");

        try {

            // the new record will be appended to the end of the file.
            long pos = raf.length();

            if (pos + nbytes > Integer.MAX_VALUE) {

                throw new IOException("Would exceed int32 bytes in file.");

            }

            // the offset into the file at which the record will be written.
            final long offset = pos;

            // // extend the file to have sufficient space for this record.
            // raf.setLength(pos + nbytes);

            // write the data onto the end of the file.
            raf.getChannel().write(data, pos);

            // formulate the address that can be used to recover that record.
            return toAddr(nbytes, offset);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

//    /**
//     * Note: the delete implementation checks its arguments and makes a
//     * <em>transient</em> note that the record has been deleted but that
//     * information does NOT survive restart of the store.
//     */
//    public void delete(long addr) {
//
//        if(addr==0L) throw new IllegalArgumentException("Address is 0L");
//        
//        final int offset = Addr.getOffset(addr);
//        
//        final int nbytes = Addr.getByteCount(addr);
//
//        if(nbytes==0) {
//            
//            throw new IllegalArgumentException(
//                    "Address encodes record length of zero");
//            
//        }
//        
//        try {
//
//            if (offset + nbytes > raf.length()) {
//
//                throw new IllegalArgumentException("Address never written.");
//
//            }
//
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//        Long l = Long.valueOf(addr);
//        
//        if(deleted.contains(l)) {
//        
//            throw new IllegalArgumentException("Address was deleted in this session");
//            
//        }
//        
//        deleted.add(l);
//        
//    }

    public void force(boolean metadata) {

        try {
            
            raf.getChannel().force(metadata);
            
        } catch( IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
                
    }

    public long size() {
        
        try {
        
            return raf.length();
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
}
