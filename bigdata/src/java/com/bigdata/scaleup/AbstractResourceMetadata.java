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
 * Created on Apr 27, 2007
 */

package com.bigdata.scaleup;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.sun.org.apache.bcel.internal.generic.GETSTATIC;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractResourceMetadata implements IResourceMetadata, Externalizable {

    /**
     * The name of the resource file.
     */
    private String filename;
    
    /**
     * The size of that file in bytes.
     */
    private long nbytes;
    
    /**
     * The life-cycle state for that resource.
     */
    private ResourceState state;

    /**
     * The unique identifier for the resource.
     */
    private UUID uuid;
    
    /**
     * De-serialization constructor.
     */
    public AbstractResourceMetadata() {
        
    }

    public AbstractResourceMetadata(String filename,long nbytes,ResourceState state, UUID uuid ) {

        if (filename == null || state == null || uuid == null)
            throw new IllegalArgumentException();

        if (nbytes < 0)
            throw new IllegalArgumentException();
        
        this.filename = filename;
        
        this.nbytes = nbytes;
        
        this.state = state;
        
        this.uuid = uuid;
        
    }

    public int hashCode() {
        
        return uuid.hashCode();
        
    }
    
    /**
     * Compares two resource metadata objects for consistent state.
     */
    public boolean equals(IResourceMetadata o) {
        
        if(this == o)return true;
        
        // Note: compares UUIDs first.

        if (uuid.equals(o.getUUID()) && filename.equals(o.getFile())
                && nbytes == o.size() && state == o.state()) {

            return true;
            
        }
        
        return false;
        
    }

    final public String getFile() {
        
        return filename;
        
    }

    public final long size() {
        
        return nbytes;
        
    }

    final public ResourceState state() {
        
        return state;
        
    }

    final public UUID getUUID() {
        
        return uuid;
        
    }
    
    private static transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = ShortPacker.unpackShort(in);
        
        if (version != 0x0)
            throw new IOException("Unknown version: " + version);

        nbytes = LongPacker.unpackLong(in);
        
        state = ResourceState.valueOf(ShortPacker.unpackShort(in));
        
        uuid = new UUID(in.readLong(),in.readLong());
        
        filename = in.readUTF();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION0);
        
        LongPacker.packLong(out, nbytes);
        
        ShortPacker.packShort(out, state.valueOf());
        
        out.writeLong(uuid.getMostSignificantBits());

        out.writeLong(uuid.getLeastSignificantBits());

        out.writeUTF(filename);
        
    }

    /**
     * A human readable representation of the resource metadata.
     */
    public String toString() {
        
        return getClass().getSimpleName()+
        "{ size="+size()+
        ", state="+state()+
        ", filename="+getFile()+
        ", uuid="+getUUID()+
        "}";
        
    }
    
}
