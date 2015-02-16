/**

Copyright (C) SYSTAP, LLC 2014.  All rights reserved.

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
 * Created onJan 24, 2014
 */

package com.bigdata.rdf.model;

import com.bigdata.rdf.internal.IV;

/**
 * This class wraps a {@link BigdataStatement}
 * and provides {@link #hashCode()} and {@link #equals(Object)}
 * respecting all four fields rather than SPO as per the {@link org.openrdf.model.Statement} contract
 * @author jeremycarroll
 *
 */
public class BigdataQuadWrapper {
	
	private final BigdataStatement delegate;
	public BigdataQuadWrapper(BigdataStatement cspo) {
		delegate = cspo;
	}
	@Override
	public int hashCode() {
        if (hash == 0) {
        	
        	if ( delegate.getContext() == null ) {
        		hash = delegate.hashCode();
        	} else {
               hash = delegate.getContext().hashCode() + 31 * delegate.hashCode();
        	}
        }
        
        return hash;
	}
    private int hash = 0;
    @Override 
    public boolean equals(Object o) {
    	if (! (o instanceof BigdataQuadWrapper)) {
    		return false;
    	}
    	final BigdataStatement oo = ((BigdataQuadWrapper)o).delegate;
    	return delegate.equals(oo) && equals(delegate.getContext(),oo.getContext());
    }
    
    private boolean equals(BigdataResource a, BigdataResource b) {
		return a == b || (a != null && a.equals(b));
	}
	public BigdataStatement statement() {
    	return delegate;
    }

}
