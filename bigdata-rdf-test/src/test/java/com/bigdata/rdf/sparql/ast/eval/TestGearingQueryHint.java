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
 * Created on Sep 29, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.List;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for https://jira.blazegraph.com/browse/BLZG-2089, which
 * introduces a fresh query hint to select the gearing choice for property
 * paths.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestGearingQueryHint extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGearingQueryHint() {
    }

    /**
     * @param name
     */
    public TestGearingQueryHint(String name) {
        super(name);
    }

    /**
     * Simple s p* ?o pattern without user-defined gearing hint.
     */
    public void test_hint_gearing_none_01() throws Exception {

        new TestHelper(
        	"hint-gearing-none-01",// testURI,
            "hint-gearing-none-01.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-01.srx"// resultFileURL
        ).runTest();
    }
    
    /**
     * Simple s p* ?o pattern with user-defined forward gearing hint.
     */
    public void test_hint_gearing_forward_01() throws Exception {

    	final TestHelper h = new TestHelper(
        	"hint-gearing-forward-01",// testURI,
            "hint-gearing-forward-01.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-01.srx"// resultFileURL
        );
        
    	// TODO: assert query hint is effective
            
    	h.runTest();
    }
    
    /**
     * Simple s p* ?o pattern with user-defined reverse gearing hint.
     */
    public void test_hint_gearing_reverse_01() throws Exception {

        final TestHelper h = new TestHelper(
        	"hint-gearing-reverse-01",// testURI,
            "hint-gearing-reverse-01.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-01.srx"// resultFileURL
        );
        
    	// TODO: assert query hint is effective
        
        h.runTest();
    }
   
}
