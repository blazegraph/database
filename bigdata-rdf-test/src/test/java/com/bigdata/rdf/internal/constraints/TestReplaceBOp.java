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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.internal.constraints;

import com.bigdata.bop.Constant;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ProxyTestCase;

/**
 * Test suite for {@link ReplaceBOp}.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestReplaceBOp extends ProxyTestCase {

//	private static final Logger log = Logger.getLogger(TestSubstrBOp.class);
	
    /**
     * 
     */
    public TestReplaceBOp() {
        super();
    }

    /**
     * @param name
     */
    public TestReplaceBOp(String name) {
        super(name);
    }
    
//    @Override
//    public Properties getProperties() {
//    	final Properties props = super.getProperties();
//    	props.setProperty(BigdataSail.Options.INLINE_DATE_TIMES, "true");
//    	return props;
//    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_bop() {
        
        final AbstractTripleStore db = getStore();

        try {

            final BigdataValueFactory vf = db.getValueFactory();
            
            final ListBindingSet emptyBindingSet = new ListBindingSet();

            // replace("abcd", "b", "Z") -> "aZcd"
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral("aZcd"));

                final IV var = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abcd"));
                
                final IV pattern = DummyConstantNode.toDummyIV(vf
                        .createLiteral("b"));
                
                final IV replacement = DummyConstantNode.toDummyIV(vf
                        .createLiteral("Z"));
                
                final IV actual = new ReplaceBOp(//
                        new Constant<IV>(var), //
                        new Constant<IV>(pattern), //
                        new Constant<IV>(replacement), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

            // replace("abab", "B", "Z","i") -> "aZaZ"
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral("aZaZ"));

                final IV var = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abab"));
                
                final IV pattern = DummyConstantNode.toDummyIV(vf
                        .createLiteral("B"));
                
                final IV replacement = DummyConstantNode.toDummyIV(vf
                        .createLiteral("Z"));
                
                final IV flags = DummyConstantNode.toDummyIV(vf
                        .createLiteral("i"));
                
                final IV actual = new ReplaceBOp(//
                        new Constant<IV>(var), //
                        new Constant<IV>(pattern), //
                        new Constant<IV>(replacement), //
                        new Constant<IV>(flags), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

            // replace("abab", "B.", "Z","i") -> "aZb"
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral("aZb"));

                final IV var = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abab"));
                
                final IV pattern = DummyConstantNode.toDummyIV(vf
                        .createLiteral("B."));
                
                final IV replacement = DummyConstantNode.toDummyIV(vf
                        .createLiteral("Z"));
                
                final IV flags = DummyConstantNode.toDummyIV(vf
                        .createLiteral("i"));
                
                final IV actual = new ReplaceBOp(//
                        new Constant<IV>(var), //
                        new Constant<IV>(pattern), //
                        new Constant<IV>(replacement), //
                        new Constant<IV>(flags), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }
}
