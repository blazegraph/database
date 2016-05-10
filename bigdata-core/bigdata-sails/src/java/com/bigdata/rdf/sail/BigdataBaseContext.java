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
package com.bigdata.rdf.sail;

import com.bigdata.journal.IIndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Context object provides access to the {@link IIndexManager}.
 * 
 * @author Martyn Cutcher
 */
public class BigdataBaseContext {

//	static private final Logger log = Logger.getLogger(BigdataBaseContext.class); 

	private final IIndexManager m_indexManager;
	
    public BigdataBaseContext(final IIndexManager indexManager) {

		if (indexManager == null)
			throw new IllegalArgumentException();

		m_indexManager = indexManager;

	}

	public IIndexManager getIndexManager() {

	    return m_indexManager;
	    
	}

    /**
     * Return <code>true</code> iff the {@link IIndexManager} is an
     * {@link IBigdataFederation} and {@link IBigdataFederation#isScaleOut()}
     * reports <code>true</code>.
     */
    public boolean isScaleOut() {

        return m_indexManager instanceof IBigdataFederation
                && ((IBigdataFederation<?>) m_indexManager).isScaleOut();
	    
	}

}
