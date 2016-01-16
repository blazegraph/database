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
package com.bigdata.journal;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.io.LongPacker;
import com.bigdata.util.Bytes;

/**
 * A helper class for (de-)serializing the root addresses.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo we could use run length encoding to write only the used roots.
 */
public class CommitRecordSerializer {

    public static final int VERSION0 = 0x0;
    
    public static final transient CommitRecordSerializer INSTANCE = new CommitRecordSerializer();
    
    public byte[] serialize(ICommitRecord commitRecord) {

        final long timestamp = commitRecord.getTimestamp();

        final long commitCounter = commitRecord.getCommitCounter();

        final int n = commitRecord.getRootAddrCount();
        
        try {
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    n * Bytes.SIZEOF_LONG);
            
            DataOutputStream dos = new DataOutputStream(baos);
            
            dos.writeInt(VERSION0);

            dos.writeLong(timestamp);

            dos.writeLong(commitCounter);
            
            LongPacker.packLong((DataOutput)dos, n);
            
            for(int i=0; i<n; i++) {
  
                dos.writeLong(commitRecord.getRootAddr(i));
//                LongPacker.packLong(dos, commitRecord.getRootAddr(i));
                
            }
            
            return baos.toByteArray();
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }
    
    public ICommitRecord deserialize(final ByteBuffer buf) {

        try {

            final ByteBufferInputStream bbis = new ByteBufferInputStream(buf);

            final DataInputStream dis = new DataInputStream(bbis);

            final int version = dis.readInt();

            if (version != VERSION0)
                throw new RuntimeException("Unknown version: " + version);

            final long timestamp = dis.readLong();
            
            final long commitCounter = dis.readLong();
            
            final int n = (int)LongPacker.unpackLong((DataInput)dis);

            final long[] roots = new long[n];

            for (int i = 0; i < n; i++) {

                roots[i] = dis.readLong();
//                roots[i] = LongPacker.unpackLong(dis);

            }
            
            return new CommitRecord(timestamp,commitCounter,roots);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
}
