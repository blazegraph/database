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
/*
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.rdf.filter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase2;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.LexiconConfiguration;
import com.bigdata.rdf.internal.NoExtensionFactory;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Unit tests for {@link HTreeDistinctFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTreeDistinctFilter extends TestCase2 {

    /**
     * 
     */
    public TestHTreeDistinctFilter() {
    }

    /**
     * @param name
     */
    public TestHTreeDistinctFilter(String name) {
        super(name);
    }

    /**
     * TODO Write tests.
     */
    public void test_something() {

        final RDFSVocabulary vocab = new RDFSVocabulary(getName()/*namespace*/);
        
        vocab.init();
        
        final ILexiconConfiguration<BigdataValue> lexConf = new LexiconConfiguration<BigdataValue>(//
                true,//inlineXSDDatatypeLiterals,
                true,//inlineTextLiterals, 
                60,//maxInlineTextLength,
                true,//inlineBNodes,
                false,//inlineDateTimes,
                true,//rejectInvalidXSDValues,
                new NoExtensionFactory(),// xFactory
                vocab // vocab
                );
        
        lexConf.initExtensions(null/*LexiconRelation*/);

        final IV rdfType = lexConf.createInlineIV(RDF.TYPE);
        final IV rdfLabel = lexConf.createInlineIV(RDFS.LABEL);
        final IV person = lexConf.createInlineIV(FOAFVocabularyDecl.Person);
        final IV mike = lexConf.createInlineIV(new URIImpl("http://www.bigdata.com/Mike"));
        final IV bryan = lexConf.createInlineIV(new URIImpl("http://www.bigdata.com/Bryan"));
        final IV labelMike = lexConf.createInlineIV(new LiteralImpl("Mike"));
        final IV labelBryan = lexConf.createInlineIV(new LiteralImpl("Bryan"));
        
        final List<IV> list = new LinkedList<IV>();
        
        list.add(mike);
        list.add(rdfType);
        list.add(person);

        list.add(bryan);
        list.add(rdfType);
        list.add(person);

        list.add(mike);
        list.add(rdfLabel);
        list.add(labelMike);

        list.add(bryan);
        list.add(rdfLabel);
        list.add(labelBryan);

        final IV[] expected = new IV[] {//
                mike, bryan,//
                rdfType, rdfLabel,//
                person,//
                labelMike, labelBryan //
        };

        // Iterator should visit the disinct IVs.
        final Iterator<IV> actual = HTreeDistinctFilter.newInstance().filter(
                list.iterator(), null/* context */);
        
        assertSameIteratorAnyOrder(expected, actual);
        
    }

}
