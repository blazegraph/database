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
package com.bigdata.rdf.internal;

import org.openrdf.model.ValueFactory;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class for inline RDF blank nodes. Blank nodes MUST be based on UUIDs or
 * some other numeric in order to be inlined.
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 * 
 * @see AbstractTripleStore.Options
 */
abstract public class AbstractBNodeIV<V extends BigdataBNode, T> extends
        AbstractInlineIV<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = -4560216387427028030L;
    
    public AbstractBNodeIV(DTE dte) {

        super(VTE.BNODE, dte);

    }

    public V asValue(final LexiconRelation lex) {
    	final ValueFactory f = lex.getValueFactory();
        final V bnode = (V) f.createBNode(stringValue());
        bnode.setIV(this);
        return bnode;
    }

}