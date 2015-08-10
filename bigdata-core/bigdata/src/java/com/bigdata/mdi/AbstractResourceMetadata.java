/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Apr 27, 2007
 */

package com.bigdata.mdi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.ShortPacker;
import com.bigdata.journal.Journal;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.Params;

/**
 * Base class for {@link IResourceMetadata} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractResourceMetadata implements IResourceMetadata,
        Externalizable, Params {

    static final protected Logger log = Logger
            .getLogger(AbstractResourceMetadata.class);
    
    /**
     * The name of the resource file.
     */
    private String filename;
    
//    /**
//     * The size of that file in bytes.
//     */
//    private long nbytes;

    /**
     * The unique identifier for the resource.
     */
    private UUID uuid;
    
    /**
     * The commit time associated with the described index. When the index is an
     * {@link IndexSegment} this is the commit time of the view from which that
     * {@link IndexSegment} was generated. When the index is a {@link BTree} on
     * a {@link Journal}, the commit time is the commit time associated with
     * the {@link BTree} revision of interest.
     */
    private long createTime;

    /**
     * @see #VERSION1
     */
    private long commitTime;

    public Map<String, Object> getParams() {

        final Map<String, Object> v = new HashMap<String, Object>();

        v.put("UUID", uuid);

        v.put("filename", filename);

        v.put("createTime", createTime);

        return v;
            
    }
    
    /**
     * De-serialization constructor.
     */
    public AbstractResourceMetadata() {
        
    }

    protected AbstractResourceMetadata(final String filename, //long nbytes,
            final UUID uuid, final long createTime, final long commitTime) {

        if (filename == null || uuid == null)
            throw new IllegalArgumentException();

//        if (nbytes <= 0)
//            throw new IllegalArgumentException(
//                    "Store file size is non-positive");

        this.filename = filename;
        
//        this.nbytes = nbytes;
        
        this.uuid = uuid;

        this.createTime = createTime;
        
        if (createTime == 0L) {
          
            throw new IllegalArgumentException("Create time is zero? : " + this);
          
        }
        
        this.commitTime = commitTime;
      
    }

    final public int hashCode() {
        
        return uuid.hashCode();
        
    }
    
    /**
     * Note: The JDK {@link HashMap} implementation requires that we define this
     * method in order for {@link HashMap#get(Object)} to work correctly!
     */
    final public boolean equals(final Object o) {
        
        return equals((IResourceMetadata)o);
        
    }
    
    /**
     * Compares two resource metadata objects for consistent state.
     */
    final public boolean equals(final IResourceMetadata o) {
        
        if (this == o)
            return true;
        
        // Note: compares UUIDs first.

        if (uuid.equals(o.getUUID()) && filename.equals(o.getFile())
//                && nbytes == o.size() 
                && createTime == o.getCreateTime()) {

            return true;
            
        }
        
        return false;
        
    }

    final public String getFile() {
        
        return filename;
        
    }

//    public final long size() {
//        
//        return nbytes;
//        
//    }

    final public UUID getUUID() {
        
        return uuid;
        
    }

    final public long getCreateTime() {
        
        return createTime;
        
    }

    final public long getCommitTime() {
        
        return commitTime;
        
    }
    
    /**
     * The original version.
     */
    private static final transient short VERSION0 = 0x0;

    /**
     * This version supports a commitTime field. That field is used when a view
     * must be defined which reads on a specific commit record in a journal. For
     * VERSION0, this field was not defined. Prior to version1, all references to
     * a historical journal were interpreted as references to the lastCommitTime
     * in the journal by the {@link IndexManager}.
     */
    private static final transient short VERSION1 = 0x1;
    
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = ShortPacker.unpackShort(in);

        switch (version) {
        case VERSION0:
        case VERSION1:
            break;
        default:
            throw new IOException("Unknown version: " + version);
        }

//        nbytes = LongPacker.unpackLong(in);
        
        uuid = new UUID(in.readLong(),in.readLong());
        
        createTime = in.readLong();
        
        if (version >= VERSION1) {

            // the specified value.
            commitTime = in.readLong();
            
        } else if(this instanceof SegmentMetadata) {
            
            // these are the always the same for an index segment.
            commitTime = createTime;
            
        } else {
            
            // no value was specified.
            commitTime = 0L;
            
        }
        
        filename = in.readUTF();

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION1);
        
//        LongPacker.packLong(out, nbytes);
        
        out.writeLong(uuid.getMostSignificantBits());

        out.writeLong(uuid.getLeastSignificantBits());

        out.writeLong(createTime);
        
        out.writeLong(commitTime);
        
        out.writeUTF(filename);
        
    }

    /**
     * A human readable representation of the resource metadata.
     */
    public String toString() {
        
        return getClass().getSimpleName()+
        "{filename="+getFile()+
        ",uuid="+getUUID()+
        ",createTime="+getCreateTime()+
        ",commitTime="+getCommitTime()+
        "}";
        
    }
    
}
