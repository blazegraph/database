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