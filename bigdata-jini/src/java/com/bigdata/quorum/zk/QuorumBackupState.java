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

/**
 * A object whose state indicates the commitCounter of (a) the last full backup;
 * and (b) the last incremental backup for the highly available logical service 
 * associated with this quorum.
 * 
 * @see ZKQuorum#QUORUM_BACKUP
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: QuorumServiceState.java 4069 2011-01-09 20:58:02Z thompsonbry $
 */
public class QuorumBackupState implements Externalizable {

    private static final long serialVersionUID = 1L;

    /**
     * The initial version. The version appears as the first byte. The remaining
     * fields for this version are: the {@link #serviceUUID} written as the most
     * significant bits (long) followed by least significant bits (long).
     */
    protected static final byte VERSION0 = 0;

    private long inc, full;

    public String toString() {
        return getClass().getName() + //
                "{inc=" + inc + ", full=" + full + //
                "}";
    }

    /**
     * Deserialization constructor.
     */
    public QuorumBackupState() {
        
    }

    public QuorumBackupState(final long inc, final long full) {

        if (inc <= 0L)
            throw new IllegalArgumentException();
        if (full <= 0L)
            throw new IllegalArgumentException();

        this.inc = inc;
        this.full = full;

    }

    public long inc() {
        return inc;
    }

    public long full() {
        return full;
    }

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();

        switch (version) {
        case VERSION0: {
            inc = in.readLong();
            full = in.readLong();
            break;
        }
        default:
            throw new IOException("Unknown version: " + version);
        }

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(VERSION0);

        out.writeLong(inc);

        out.writeLong(full);

    }

}
