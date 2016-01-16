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
 * Created on Jun 8, 2011
 */

package com.bigdata.test;

import java.util.Random;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;

/**
 * A factory for mock {@link IV}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockTermIdFactory {

    /**
     * 
     */
    public MockTermIdFactory() {
//        helper = new TermsIndexHelper();
    }

//    final private TermsIndexHelper helper;
    final private Random r = new Random();
//    private int nextHashCode = 1;
    private long nextTermId = 1L;
    
    /**
     * Factory for {@link IV}s.
     */
    public IV<?,?> newTermId(final VTE vte) {

//        final int hashCode = nextHashCode++;
//        
//        // the math here is just to mix up the counter values a bit.
//        final byte counter = (byte) ((nextHashCode + 12) % 7);
//
//        return new BlobIV(vte, hashCode, counter);
        
        return new TermId(vte, nextTermId++);
        
    }

    /**
     * Random distribution of different {@link VTE} types.
     */
    public IV<?,?> newTermId() {

        final VTE vte;
        switch (r.nextInt(3)) {
        case 0:
            vte = VTE.URI;
            break;
        case 1:
            vte = VTE.BNODE;
            break;
        case 2:
            vte = VTE.LITERAL;
            break;
        case 3:
            vte = VTE.STATEMENT;
            break;
        default:
            throw new AssertionError();
        }
        
        return newTermId(vte);
        
    }

    /**
     * Random distribution without SIDs.
     */
    public IV<?,?> newTermIdNoSids() {

        final VTE vte;
        switch (r.nextInt(2)) {
        case 0:
            vte = VTE.URI;
            break;
        case 1:
            vte = VTE.BNODE;
            break;
        case 2:
            vte = VTE.LITERAL;
            break;
//        case 3:
//            vte = VTE.STATEMENT;
//            break;
        default:
            throw new AssertionError();
        }
        
        return newTermId(vte);
        
    }

    /**
     * {@link TermId} having the termId. This is useful when you want to control
     * the distribution of the {@link IV}s. There is no guarantee that the
     * {@link TermIV} is unique.
     * 
     * @param vte
     * @param termId
     * @return
     */
    public IV<?,?> newTermId(final VTE vte, final long termId) {

//        final byte counter = 0;
//        
//        return new BlobIV(vte, hashCode, counter);

        return new TermId(vte, termId);
        
    }
    
}
