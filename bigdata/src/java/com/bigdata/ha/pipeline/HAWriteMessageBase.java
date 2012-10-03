/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.ha.pipeline;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.journal.ha.HAWriteMessage;

/**
 * Base class for RMI messages used to communicate metadata about a raw data
 * transfer occurring on a socket channel.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see HASendService
 * @see HAReceiveService
 */
public class HAWriteMessageBase implements Externalizable {
	
	/**
     * 
     */
    private static final long serialVersionUID = -6807744466616574690L;

    /** The #of bytes of data to be transfered. */
    private int sze;

    /** The Alder32 checksum of the bytes to be transfered. */
    private int chk;

    /**
     * 
     * @param sze
     *            The #of bytes of data to be transfered.
     * @param chk
     *            The Alder32 checksum of the bytes to be transfered.
     */
    public HAWriteMessageBase(final int sze, final int chk) {

        if (sze <= 0)
            throw new IllegalArgumentException();
		
        this.sze = sze;
		
        this.chk = chk;
        
	}
	
    /**
     * Deserialization constructor.
     */
	public HAWriteMessageBase() {}

	/** The #of bytes of data to be transfered. */
	public int getSize() {

	    return sze;
	    
	}

	/** The Alder32 checksum of the bytes to be transfered. */
	public int getChk() {

	    return chk;
	    
	}
	
    public String toString() {

        return super.toString() + "{size=" + sze + ",chksum=" + chk + "}";
        
    }
    
    @Override
    public boolean equals(final Object obj) {

        if (this == obj)
            return true;
        
        if (!(obj instanceof HAWriteMessageBase))
            return false;
        
        final HAWriteMessageBase t = (HAWriteMessageBase) obj;

        return sze == t.getSize() && chk == t.getChk();

    }

    @Override
    public int hashCode() {

        // checksum is a decent hash code if given otherwise the size.
        return chk == 0 ? sze : chk;

    }
    
    private static final transient short VERSION0 = 0x0;
    
    private static final transient short currentVersion = VERSION0;
    
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = in.readShort();

        if (version != VERSION0)
            throw new RuntimeException("Bad version for serialization");

        sze = in.readInt();
        
        chk = in.readInt();
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        
        out.writeShort(currentVersion);
    	
        out.writeInt(sze);
        
        out.writeInt(chk);
        
    }

}
