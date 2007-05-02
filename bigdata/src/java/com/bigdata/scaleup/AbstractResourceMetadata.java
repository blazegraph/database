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

}
