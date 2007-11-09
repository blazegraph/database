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
package com.bigdata.journal;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.rawstore.Bytes;

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
            
            LongPacker.packLong(dos, n);
            
            for(int i=0; i<n; i++) {
                
                LongPacker.packLong(dos, commitRecord.getRootAddr(i));
                
            }
            
            return baos.toByteArray();
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }
    
    public ICommitRecord deserialize(ByteBuffer buf) {

        try {

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);

            DataInputStream dis = new DataInputStream(bbis);

            final int version = dis.readInt();

            if (version != VERSION0)
                throw new RuntimeException("Unknown version: " + version);

            final long timestamp = dis.readLong();
            
            final long commitCounter = dis.readLong();
            
            final int n = (int)LongPacker.unpackLong(dis);

            final long[] roots = new long[n];

            for (int i = 0; i < n; i++) {

                roots[i] = LongPacker.unpackLong(dis);

            }
            
            return new CommitRecord(timestamp,commitCounter,roots);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
}