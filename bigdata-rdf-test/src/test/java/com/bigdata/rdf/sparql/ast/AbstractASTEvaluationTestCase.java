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
 * Created on Aug 29, 2011
 */
/* Portions of this file are:
 * 
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Aduna BSD-style license.
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractASTEvaluationTestCase extends AbstractQueryEngineTestCase {

    private static final Logger log = Logger
            .getLogger(AbstractASTEvaluationTestCase.class);

    /**
     * 
     */
    public AbstractASTEvaluationTestCase() {
    }

    public AbstractASTEvaluationTestCase(String name) {
        super(name);
    } 

    protected AbstractTripleStore store = null;

    protected BigdataValueFactory valueFactory = null;

    protected String baseURI = null;

    private BOpContextBase context = null;
    
    /**
     * Return the context for evaluation of {@link IValueExpression}s during
     * query optimization.
     * 
     * @return The context that can be used to resolve the
     *         {@link ILexiconConfiguration} and {@link LexiconRelation} for
     *         evaluation if {@link IValueExpression}s during query
     *         optimization. (During query evaluation this information is passed
     *         into the pipeline operators by the {@link ContextBindingSet}.)
     * 
     * @see BLZG-1372
     */
    public BOpContextBase getBOpContext() {

        return context;
        
    }
    
    @Override
    protected void setUp() throws Exception {
        
        super.setUp();
        
        store = getStore(getProperties());

        context = new BOpContextBase(null/* fed */, store.getIndexManager());
        
        valueFactory = store.getValueFactory();
        
        /*
         * Note: This needs to be an absolute URI.
         */
        
        baseURI = "http://www.bigdata.com";
    }
    
    @Override
    protected void tearDown() throws Exception {
        
        if (store != null) {
        
            store.__tearDownUnitTest();
            
            store = null;
        
        }

        context = null;
        
        valueFactory = null;
        
        baseURI = null;
        
        super.tearDown();
        
    }

    protected void enableDeleteMarkersInIndes() {
       final SPORelation rel = store.getSPORelation();
       rel.getPrimaryIndex().getIndexMetadata().setDeleteMarkers(true);
    }
    
    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");

        // TM not available with quads.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");

//        // override the default vocabulary.
//        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
//                NoVocabulary.class.getName());

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        return properties;

    }

    protected AbstractTripleStore getStore(final Properties properties) {

        final String namespace = "kb";

        // create/re-open journal.
        final Journal journal = new Journal(properties);

        final LocalTripleStore lts = new LocalTripleStore(journal, namespace,
                ITx.UNISOLATED, properties);

        lts.create();

        return lts;

    }

    
    static protected void assertSameAST(final IQueryNode expected,
            final IQueryNode actual) {

        if (!expected.equals(actual)) {

            log.error("expected: " + BOpUtility.toString((BOp) expected));
            log.error("actual  : " + BOpUtility.toString((BOp) actual));

            AbstractQueryEngineTestCase.diff((BOp) expected, (BOp) actual);

            // No difference was detected?
            throw new AssertionError();
//            fail("expected:\n" + BOpUtility.toString((BOp) expected)
//                    + "\nactual:\n" + BOpUtility.toString((BOp) actual));

        } else if(log.isInfoEnabled()) {

            log.info(BOpUtility.toString((BOp) expected));
            
        }

    }

    protected static Set<IVariable<?>> asSet(final String[] vars) {

        final Set<IVariable<?>> set = new LinkedHashSet<IVariable<?>>();

        for (String s : vars) {

            set.add(Var.var(s));

        }

        return set;

    }

    protected static Set<IVariable<?>> asSet(final IVariable<?>[] vars) {

        final Set<IVariable<?>> set = new LinkedHashSet<IVariable<?>>();

        for (IVariable<?> var : vars) {

            set.add(var);

        }

        return set;

    }

    static protected final Set<VarNode> asSet(final VarNode [] a) {
        
        return new LinkedHashSet<VarNode>(Arrays.asList(a));
        
    }
    
    static protected final Set<FilterNode> asSet(final FilterNode [] a) {
        
        return new LinkedHashSet<FilterNode>(Arrays.asList(a));
        
    }

    static protected final Set<Integer> asSet(final Integer[] a) {
        
        return new LinkedHashSet<Integer>(Arrays.asList(a));
        
    }

//    /**
//     * Return a mock IV for the value.
//     */
//    @SuppressWarnings("unchecked")
//    protected IV<BigdataValue, ?> mockIV(final BigdataValue value) {
//
//        IV iv = store.getLexiconRelation().getInlineIV(value);
//
//        if (iv == null) {
//
//            iv = (IV<BigdataValue, ?>) TermId.mockIV(VTE.valueOf(value));
//            
//            iv.setValue(value);
//
//        }
//
//        return iv;
//
//    }

    /**
     * Return a (Mock) IV for a Value.
     * 
     * @param v
     *            The value.
     *            
     * @return The Mock IV.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected IV makeIV(final Value v) {
        final BigdataValue bv = store.getValueFactory().asValue(v);
        final IV iv = TermId.mockIV(VTE.valueOf(v));
        iv.setValue(bv);
        return iv;
    }
    
}
