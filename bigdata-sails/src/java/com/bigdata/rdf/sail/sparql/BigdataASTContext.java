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
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataASTContext {

    protected final AbstractTripleStore tripleStore;

    protected final LexiconRelation lexicon;
    
    protected final String lex;

    protected final ILexiconConfiguration<BigdataValue> conf;

    protected final BigdataValueFactory valueFactory;

    public BigdataASTContext(final AbstractTripleStore tripleStore) {

        this.tripleStore = tripleStore;

        this.valueFactory = tripleStore.getValueFactory();

        this.lexicon = tripleStore.getLexiconRelation();

        this.lex = lexicon.getNamespace();

        this.conf = lexicon.getLexiconConfiguration();

    }

}
