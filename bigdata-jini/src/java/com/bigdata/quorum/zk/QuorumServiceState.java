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
/*
 * Created on Jun 29, 2010
 */

package com.bigdata.quorum.zk;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import com.bigdata.jini.start.BigdataZooDefs;

/**
 * A object whose state contains the address at which a service listens for
 * replicated writes sent along the write pipeline. Instances of this object are
 * written into the data for the {@link BigdataZooDefs#QUORUM_PIPELINE} leaf
 * znodes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QuorumServiceState implements Externalizable {

    private static final long serialVersionUID = 1L;

    /**
     * The initial version. The version appears as the first byte. The remaining
     * fields for this version are: the {@link #serviceUUID} written as the most
     * significant bits (long) followed by least significant bits (long).
     */
    protected static final byte VERSION0 = 0;

    private UUID serviceUUID;

    public String toString() {
        return getClass().getName() + //
                "{serviceUUID=" + serviceUUID() + //
                "}";
    }

    /**
     * Deserialization constructor.
     */
    public QuorumServiceState() {
        
    }

    public QuorumServiceState(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        this.serviceUUID = serviceUUID;

    }

    public UUID serviceUUID() {
        return serviceUUID;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();

        switch (version) {
        case VERSION0: {
            serviceUUID = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);
            break;
        }
        default:
            throw new IOException("Unknown version: " + version);
        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.write(VERSION0);

        out.writeLong(serviceUUID.getMostSignificantBits());
        
        out.writeLong(serviceUUID.getLeastSignificantBits());

    }

}
