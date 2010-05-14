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

package com.bigdata.journal.ha;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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
public class HAWriteMessage implements Externalizable {
	
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
    public HAWriteMessage(final int sze, final int chk) {
        if (sze <= 0)
            throw new IllegalArgumentException();
		this.sze = sze;
		this.chk = chk;
	}
	
    /**
     * Deserialization constructor.
     */
	public HAWriteMessage() {}

	public int getSize() {
		return sze;
	}
	
	public int getChk() {
		return chk;
	}
	
    public String toString() {

        return super.toString() + "{size=" + sze + ",chksum=" + chk + "}";
        
    }
    
//	public boolean usePrefixWrites() {
//		return prefixWrites;
//	}
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		sze = in.readInt();
		chk = in.readInt();
//		prefixWrites = in.readBoolean();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(sze);
		out.writeInt(chk);
//		out.writeBoolean(prefixWrites);
	}	
}
