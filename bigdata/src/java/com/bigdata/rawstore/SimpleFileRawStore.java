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
 * Created on Jan 31, 2007
 */

package com.bigdata.rawstore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;


/**
 * A simple persistent unbuffered implementation backed by a file. The maximum
 * size of the file is restricted to {@link Integer#MAX_VALUE} bytes since we
 * must code the offset into the file using {@link Addr#toLong(int, int)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo the commit protocol is not implemented yet.
 */
public class SimpleFileRawStore implements IRawStore {

    private boolean open = true;
    
    public final File file;
    protected final RandomAccessFile raf;
    
    /**
     * This provides a purely transient means to identify deleted records.  This
     * data does NOT survive restart of the store. 
     */
    private final Set<Long> deleted = new HashSet<Long>();

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

    public ByteBuffer read(long addr, ByteBuffer dst) {

        if (addr == 0L)
            throw new IllegalArgumentException("Address is 0L");

        final int offset = Addr.getOffset(addr);

        final int nbytes = Addr.getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(
                    "Address encodes record length of zero");

        }

        if(deleted.contains(addr)) {
            
            throw new IllegalArgumentException("Address was deleted in this session");
            
        }

        try {

            if (offset + nbytes > raf.length()) {

                throw new IllegalArgumentException("Address never written.");

            }

            if (dst != null && dst.remaining() >= nbytes) {

                // copy exactly this many bytes.

                dst.limit(dst.position() + nbytes);

                // copy into the caller's buffer.
                
                raf.getChannel().read(dst,(long)offset);

                // flip for reading.

                dst.flip();

                // the caller's buffer.

                return dst;

            } else {

                // allocate a new buffer of the exact capacity.

                dst = ByteBuffer.allocate(nbytes);

                // copy the data into the buffer.

                raf.getChannel().read(dst,(long)offset);

                // flip for reading.

                dst.flip();

                // return the buffer.

                return dst;

            }

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
            final int offset = (int) pos;

            // // extend the file to have sufficient space for this record.
            // raf.setLength(pos + nbytes);

            // write the data onto the end of the file.
            raf.getChannel().write(data, pos);

            // formulate the address that can be used to recover that record.
            return Addr.toLong(nbytes, offset);

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
    
}
