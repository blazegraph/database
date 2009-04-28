/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 28, 2009
 */

package com.bigdata.rdf.iris;

import java.util.Collection;
import java.util.Properties;
import org.deri.iris.api.IProgramOptimisation.Result;
import org.deri.iris.api.basics.IAtom;
import org.deri.iris.api.basics.IQuery;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.optimisations.magicsets.MagicSets;
import org.deri.iris.terms.TermFactory;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rules.AbstractInferenceEngineTestCase;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

/**
 * Test suite for IRIS-based truth maintenance on delete.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIRIS extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestIRIS() {
        super();
    }

    /**
     * @param name
     */
    public TestIRIS(String name) {
        super(name);
    }

    /**
     * We are trying to eliminate the use of justification chains inside the
     * database.  These justification chains are used to determine whether or
     * not a statement is grounded by other facts in the database.  To determine 
     * this without justifications, we could use a magic sets optimization
     * against the normal inference rules using the statement to test as the
     * query. 
     */
    public void testRetractionWithIRIS() {
        
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI U = f.createURI("http://www.bigdata.com/U");
            final BigdataURI V = f.createURI("http://www.bigdata.com/V");
            final BigdataURI X = f.createURI("http://www.bigdata.com/X");
            final BigdataURI sco = f.asValue(RDFS.SUBCLASSOF);
            
            
            // set the stage
            // U sco V and V sco X implies U sco X
            // let's pretend all three statements were originally in the
            // database, and { U sco X } is retracted, leaving only the other
            // two.
            // we should be able to create an adorned program based on our
            // normal inference rules using the "retracted" statement as the
            // query.
            
            store.addStatement(U, sco, V);
            
            store.addStatement(V, sco, X);
            
            if (log.isInfoEnabled())
                log.info("\n\nstore:\n"
                        + store.dumpStore(store,
                                true, true, true, true));
            
            // now get the program from the inference engine
            
            BaseClosure closure = store.getClosureInstance();
            
            MappedProgram program = closure.getProgram(
                    store.getSPORelation().getNamespace(), null
                    );
            
            // now we convert the bigdata program into an IRIS program

            Collection<IRule> rules = null; // convertToIRIS(program);
            
            // then we create a query for the fact we are looking for
            
            IBasicFactory BASIC = BasicFactory.getInstance();
            
            ITermFactory TERM = TermFactory.getInstance();
            
            IAtom atom = BASIC.createAtom(
                BASIC.createPredicate(sco.stringValue(), 2),
                BASIC.createTuple(
                    TERM.createString(U.stringValue()), 
                    TERM.createString(X.stringValue())
                    )
                );
            
            IQuery query = BASIC.createQuery(
                BASIC.createLiteral(
                    true, // positive 
                    atom
                    )
                );
            
            // create the magic sets optimizer
            
            MagicSets magicSets = new MagicSets();
            
            Result result = magicSets.optimise(rules, query);
            
            // now we take the optimized set of rules and convert it back to a
            // bigdata program
            
            MappedProgram magicProgram = null; // convertToBigdata(result.rules);
            
            // then we somehow run the magic program and see if the fact in
            // question exists in the resulting closure
            
            // ??????
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}
