/*

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.store;

import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;

/**
 * A simple class that represents a triple (or quad) pattern.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/866" > Efficient batch remove of
 *      a collection of triple patterns </a>
 */
public class BigdataTriplePattern {

//    private static final long serialVersionUID = 1L;

    private final BigdataResource s;
    private final BigdataURI p;
    private final BigdataValue o;
    private final BigdataResource c;

    public BigdataTriplePattern(final BigdataResource subject,
            final BigdataURI predicate, final BigdataValue object) {

        this(subject, predicate, object, (BigdataResource) null);

    }

    public BigdataTriplePattern(final BigdataResource subject,
            final BigdataURI predicate, final BigdataValue object,
            final BigdataResource context) {

        this.s = subject;

        this.p = predicate;
        
        this.o = object;
        
        this.c = context;
        
    }

    final public BigdataResource getSubject() {

        return s;
        
    }
    
    final public BigdataURI getPredicate() {

        return p;
        
    }

    final public BigdataValue getObject() {
     
        return o;
        
    }

    final public BigdataResource getContext() {
        
        return c;
        
    }
    
}
