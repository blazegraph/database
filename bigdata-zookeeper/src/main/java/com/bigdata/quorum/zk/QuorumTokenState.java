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
 * Created on Jun 29, 2010
 */

package com.bigdata.quorum.zk;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.quorum.Quorum;

/**
 * A object whose state contains both the lastValidToken and the current token.
 * Instances of this object are written into the data for the
 * {@link ZKQuorum#QUORUM} znode.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QuorumTokenState implements Externalizable {

    private static final long serialVersionUID = 1L;

    /**
     * The initial version. The version appears as the first byte. The remaining
     * fields for this version are: the {@link #lastValidToken()} followed by the
     * {@link #currentToken}.
     */
    protected static final byte VERSION0 = 0;

    /**
     * This version adds the {@link #replicationFactor} field. 
     */
    protected static final byte VERSION1 = 1;

    protected static final byte currentVersion = VERSION1;
    
    private long lastValidToken;

    private long currentToken;
    
    private int replicationFactor;

    public String toString() {
        return getClass().getName() + //
                "{lastValidToken=" + lastValidToken + //
                ",currentToken=" + currentToken + //
                ",replicationFactor=" + replicationFactor + //
                "}";
    }
    
    /**
     * Deserialization constructor.
     */
    public QuorumTokenState() {
        
    }
    
    public QuorumTokenState(final long lastValidToken, final long currentToken,
            final int replicationFactor) {

        this.lastValidToken = lastValidToken;

        this.currentToken = currentToken;
        
        this.replicationFactor = replicationFactor;

        if (currentToken != Quorum.NO_QUORUM && lastValidToken != currentToken) {
            /*
             * When the current token is set, it must be the same as the last
             * valid token.
             */
            throw new IllegalArgumentException();
        }

        if (replicationFactor < 1)
            throw new IllegalArgumentException();

        if ((replicationFactor % 2) == 0)
            throw new IllegalArgumentException(
                    "replicationFactor must be odd: " + replicationFactor);

    }

    /**
     * The last valid token around which there was a quorum meet.
     */
    final public long lastValidToken() {
        
        return lastValidToken;
        
    }

    /**
     * The current token and {@value Quourm#NO_QUORUM} if the quorum is
     * not met.
     */
    final public long token() {

        return currentToken;
        
    }

    /**
     * The replication factor for this quorum (a non-negative, odd integer).
     */
    final public int replicationFactor() {

        return replicationFactor;
        
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();

        switch (version) {
        case VERSION0: {
            lastValidToken = in.readLong();
            currentToken = in.readLong();
            replicationFactor = 0; // KNOWN BAD VALUE
            break;
        }
        case VERSION1: {
            lastValidToken = in.readLong();
            currentToken = in.readLong();
            replicationFactor = in.readInt();
            break;
        }
        default:
            throw new IOException("Unknown version: " + version);
        }

    }

    /**
     * @serialData A one byte version.
     */
    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(currentVersion);

        out.writeLong(lastValidToken);

        out.writeLong(currentToken);

        if (currentVersion >= VERSION1) {

            out.writeInt(replicationFactor);

        }
        
    }

}
