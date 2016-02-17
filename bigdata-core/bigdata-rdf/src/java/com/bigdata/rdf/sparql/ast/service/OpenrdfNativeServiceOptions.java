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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import org.openrdf.query.BindingSet;

import com.bigdata.bop.IBindingSet;

/**
 * Service options base class for with JVM services which handle openrdf
 * {@link BindingSet}s rather than bigdata {@link IBindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OpenrdfNativeServiceOptions extends ServiceOptionsBase implements
        INativeServiceOptions {

    /**
     * Always returns <code>false</code>.
     */
    @Override
    final public boolean isBigdataNativeService() {
        return false;
    }

    /**
     * Always returns <code>false</code>.
     */
    @Override
    final public boolean isRemoteService() {
        return false;
    }

    /**
     * Always returns <code>false</code> (response is ignored).
     * 
     */
    @Override
    final public boolean isSparql10() {
        return false;
    }

    /**
     * Always returns <code>null</code> (response is ignored).
     * 
     */
	@Override
	final public SPARQLVersion getSPARQLVersion() {
		return null;
	}

}
