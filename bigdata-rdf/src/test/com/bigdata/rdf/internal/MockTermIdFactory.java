/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 8, 2011
 */

package com.bigdata.rdf.internal;

import java.util.Random;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.TermsIndexHelper;

/**
 * A factory to mock {@link TermId}s.
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockTermIdFactory {

    /**
     * 
     */
    public MockTermIdFactory() {
        helper = new TermsIndexHelper();
    }

    final private TermsIndexHelper helper;
    final private Random r = new Random();
    private int nextHashCode = 1;
    
    /**
     * Factory for {@link TermId}s.
     */
    public TermId newTermId(final VTE vte) {

        final int hashCode = nextHashCode++;
        
        // the math here is just to mix up the counter values a bit.
        final byte counter = (byte) ((nextHashCode + 12) % 7);
        
//        final IKeyBuilder keyBuilder = helper.newKeyBuilder();
//
//        final byte[] key = helper.makeKey(keyBuilder, vte, hashCode, counter);
//        
//        return new TermId(key);
        
		return new TermId(vte, hashCode, counter);
        
    }

    /**
     * Random distribution of different {@link VTE} types.
     */
    public TermId newTermId() {
        
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
    public TermId newTermIdNoSids() {
        
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
     * {@link TermId} having the specified hash code and a fixed counter value.
     * This is useful when you want to control the distribution of the
     * {@link TermId}s. There is no guarantee that the {@link TermId} is unique.
     * 
     * @param vte
     * @param hashCode
     * @return
     */
    public TermId newTermId(final VTE vte, final int hashCode) {

        final byte counter = 0;

//        final IKeyBuilder keyBuilder = helper.newKeyBuilder();
//
//        final byte[] key = helper.makeKey(keyBuilder, vte, hashCode, counter);
//        
//        return new TermId(key);
        
        return new TermId(vte,hashCode,counter);

    }

}
