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
 * Created on Jun 1, 2010
 */

package com.bigdata.quorum;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;

/**
 * A utility class that bundles together the Internet address and port at which
 * the downstream service will accept and relay cache blocks for the write
 * pipeline and the remote interface which is used to communicate with that
 * service using RMI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PipelineState<S extends HAGlue> implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The Internet address and port at which the downstream service will accept
     * and relay cache blocks for the write pipeline.
     */
    public InetSocketAddress addr;

    /**
     * The remote interface for the downstream service which will accept and
     * relay cache blocks from this service.
     * <p>
     * Note: In order for an instance of this class to be serializable, an
     * exported proxy for the {@link HAGlue} object must be used here rather
     * than the local object reference.
     */
    public S service;

    public PipelineState() {

    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        addr = (InetSocketAddress) in.readObject();

        service = (S) in.readObject();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeObject(addr);

        out.writeObject(service);

    }

}
