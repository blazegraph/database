/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Aug 9, 2012
 */
package com.bigdata.gom.om;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;

/**
 * Generator for globally unique URIs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class IDGenerator implements IIDGenerator {

    private final ValueFactory valueFactory;

    private final String prefix;

    private final AtomicInteger nextId = new AtomicInteger(0);

    /**
     * 
     * @param endpoint
     *            The SPARQL endpoint.
     * @param uuid
     *            The {@link UUID} for the {@link IObjectManager}. This is used
     *            to avoid collisions between different {@link IObjectManager}s.
     * @param valueFactory
     *            The {@link ValueFactory} that will be used when new
     *            {@link URI}s are generated.
     */
    public IDGenerator(final String endpoint, final UUID uuid,
            final ValueFactory valueFactory) {

        if(endpoint == null)
            throw new IllegalArgumentException();

        if(uuid == null)
            throw new IllegalArgumentException();

        if(valueFactory == null)
            throw new IllegalArgumentException();

        this.valueFactory = valueFactory;

        // Setup the prefix that we will reuse for each new URI.
        this.prefix = endpoint + "/gpo/" + uuid + "/";

    }

    @Override
    public URI genId() {

        return valueFactory.createURI(prefix + nextId.incrementAndGet());

    }

    @Override
    public URI genId(final String scope) {

        return valueFactory.createURI(prefix + scope + "/"
                + nextId.incrementAndGet());

    }

    @Override
    public void rollback() {

        nextId.set(0);
        
    }

}
