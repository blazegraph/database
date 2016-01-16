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
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ProxyTestCase;

/**
 * Test suite for {@link StrAfterBOp}.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestStrAfterBOp extends ProxyTestCase {

//	private static final Logger log = Logger.getLogger(TestSubstrBOp.class);
	
    /**
     * 
     */
    public TestStrAfterBOp() {
        super();
    }

    /**
     * @param name
     */
    public TestStrAfterBOp(String name) {
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

            // strbefore("abc","b") -> "c"
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral("c"));

                final IV arg1 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc"));
                
                final IV arg2 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("b"));
                
                final IV actual = new StrAfterBOp(//
                        new Constant<IV>(arg1), //
                        new Constant<IV>(arg2), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

            // strbefore("abc"@en,"ab") -> "c"@en
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral("c", "en"));

                final IV arg1 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc", "en"));
                
                final IV arg2 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("ab"));
                
                final IV actual = new StrAfterBOp(//
                        new Constant<IV>(arg1), //
                        new Constant<IV>(arg2), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

            // strbefore("abc"@en,"b"@cy) -> error
            {
                final IV arg1 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc", "en"));
                
                final IV arg2 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("b", "cy"));
                
                try {
	                final IV actual = new StrAfterBOp(//
	                        new Constant<IV>(arg1), //
	                        new Constant<IV>(arg2), //
	                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
	                ).get(emptyBindingSet);
	                
	                fail("should be a type error");
                } catch (SparqlTypeErrorException ex) { }

            }
            
            // strbefore("abc"^^xsd:string,"") -> ""^^xsd:string
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc", XSD.STRING));

                final IV arg1 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc", XSD.STRING));
                
                final IV arg2 = DummyConstantNode.toDummyIV(vf
                        .createLiteral(""));
                
                final IV actual = new StrAfterBOp(//
                        new Constant<IV>(arg1), //
                        new Constant<IV>(arg2), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

            // strbefore("abc","xyz") -> ""
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral(""));

                final IV arg1 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc"));
                
                final IV arg2 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("xyz"));
                
                final IV actual = new StrAfterBOp(//
                        new Constant<IV>(arg1), //
                        new Constant<IV>(arg2), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

            // strbefore("abc","bc") -> ""
            {
                final IV expected = DummyConstantNode.toDummyIV(vf
                        .createLiteral(""));

                final IV arg1 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("abc"));
                
                final IV arg2 = DummyConstantNode.toDummyIV(vf
                        .createLiteral("bc"));
                
                final IV actual = new StrAfterBOp(//
                        new Constant<IV>(arg1), //
                        new Constant<IV>(arg2), //
                        new GlobalAnnotations(vf.getNamespace(), ITx.READ_COMMITTED)//
                ).get(emptyBindingSet);

                assertEquals(expected, actual);
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }
}
