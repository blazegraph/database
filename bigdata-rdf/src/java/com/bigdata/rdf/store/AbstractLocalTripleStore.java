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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.lang.ref.SoftReference;
import java.util.Properties;
import java.util.concurrent.Executors;

import com.bigdata.journal.IIndexManager;
import com.bigdata.search.FullTextIndex;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for both transient and persistent {@link ITripleStore}
 * implementations using local storage.
 * <p>
 * This implementation presumes unisolated writes on local indices and a single
 * client writing on a local database. Unlike the {@link ScaleOutTripleStore}
 * this implementation does NOT feature auto-commit for unisolated writes. The
 * implication of this is that the client controls the commit points which means
 * that it is easier to guarentee that the KB is fully consistent since partial
 * writes can be abandoned.
 * 
 * @deprecated Is this class still required?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTripleStore extends AbstractTripleStore {

    public AbstractLocalTripleStore(Properties properties) {
        
        super(properties);

    }

    /**
     * A factory returning the singleton for the {@link FullTextIndex}.
     */
    synchronized public FullTextIndex getSearchEngine() {

        if (searchEngine == null) {

            // FIXME namespace once used by localtriplestore.
            final String namespace = "";

            searchEngine = new FullTextIndex(getProperties(), namespace,
                    getStore(), Executors
                            .newSingleThreadExecutor(DaemonThreadFactory
                                    .defaultThreadFactory()));

        }

        return searchEngine;

    }

    private FullTextIndex searchEngine;

    abstract IIndexManager getStore();

}
