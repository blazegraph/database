/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 30, 2005
 */
package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.BTree;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * @author personickm
 */
abstract class BaseAxioms implements Axioms {
    
    Set<Triple> axioms = new HashSet<Triple>();
    
    Set<String> vocabulary = new HashSet<String>();
    
    /**
     * The axioms in SPO order.
     */
    private BTree btree;

    /**
     * Used to generate keys for the {@link #btree}.
     */
    private final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(new KeyBuilder());    

    private final AbstractTripleStore db;

    /**
     * 
     * @param db
     *            The database whose lexicon will define the term identifiers
     *            for the axioms and on which the axioms will be lazily written
     *            by {@link #addAxioms()}.
     */
    protected BaseAxioms(AbstractTripleStore db)
    {
        
        if (db == null)
            throw new IllegalArgumentException();
        
        this.db = db;
  
        /*
         * @todo eagerly defining the axioms breaks a some unit tests that
         * assume that the store is still empty after the inference engine has
         * been instantiated.
         */
//        defineAxioms();
        
    }
    
    protected void addAxiom
        ( String s,
          String p,
          String o
          )
    {
        
        vocabulary.add
            ( s
              );
        
        vocabulary.add
            ( p
              );
        
        vocabulary.add
            ( o
              );
        
        axioms.add
            ( new TripleImpl
                ( new URIImpl
                      ( s
                        ),
                  new URIImpl
                      ( p
                        ),
                  new URIImpl
                      ( o
                        )
                  )
              );
        
    }
               
    public Set<Triple> getAxioms()
    {
        
        return axioms;
        
    }    
    
    public Set<String> getVocabulary()
    {
        
        return vocabulary;
        
    }

    public boolean isAxiom( Statement statement )
    {
        
        Resource subj = statement.getSubject();
        
        URI pred = statement.getPredicate();
        
        Value obj = statement.getObject();
        
        if ( subj instanceof URI && obj instanceof URI ) {
            
            return isAxiom
                ( ( URI ) subj,
                  pred,
                  ( URI ) obj
                  );
            
        }
        
        return false;
        
    }
    
    public boolean isAxiom
        ( URI s,
          URI p,
          URI o
          )
    {
        
        return axioms.contains
            ( new TripleImpl
                ( s,
                  p,
                  o
                  )
              );
        
    }
              
    public boolean isInVocabulary( URI uri )
    {
        
        return vocabulary.contains( uri.getURI() );
        
    }
    
    private class TripleImpl implements Triple {
        
        URI s;
        URI p;
        URI o;
        int hashCode;
        
        public TripleImpl
            ( URI s,
              URI p,
              URI o
              )
        {
            
            this.s = s;
            this.p = p;
            this.o = o;
            
            hashCode = 
                ( s.getURI() + "|" + 
                  p.getURI() + "|" + 
                  o.getURI()
                  ).hashCode();
            
        }
        
        public URI getS()
        {
            
            return s;
            
        }
        
        public URI getP()
        {
            
            return p;
            
        }
        
        public URI getO()
        {
            
            return o;
            
        }
        
        public boolean equals
            ( Object obj
              )
        {
         
            return 
                ( obj != null &&
                  obj instanceof Triple &&
                  s.getURI().equals( ( ( Triple ) obj ).getS().getURI() ) &&
                  p.getURI().equals( ( ( Triple ) obj ).getP().getURI() ) &&
                  o.getURI().equals( ( ( Triple ) obj ).getO().getURI() )
                  );
            
        }
        
        public int hashCode()
        {
            
            return hashCode;
            
        }
              
    }
    
    /**
     * Add the axiomatic RDF(S) triples to the store.
     */
    public void addAxioms() {

        if(btree==null) {

            /*
             * FIXME Lazy insert of axioms is probably a bad idea. The only
             * problems with doing this eagerly when the InferenceEngine is
             * instantiated is that a bunch of unit tests all presume that the
             * store is still empty after creating an InferenceEngine.  This,
             * those tests should probably be modified and the axioms defined
             * when the inference engine is instantiated.
             */

            defineAxioms();
            
        }
        
    }

    private void defineAxioms() {
        
        /*
         * Write the axioms on the database.
         * 
         * Note: if the terms for the axioms are already in the lexicon and the
         * axioms are already in the database then this will not write on the
         * database, but it will still result in the SPO[] containing the axioms
         * to be defined in MyStatementBuffer.
         */
        
        MyStatementBuffer buffer = new MyStatementBuffer(db, getAxioms().size());

        for (Iterator<Axioms.Triple> itr = getAxioms().iterator(); itr
                .hasNext();) {

            Axioms.Triple triple = itr.next();
            
            URI s = triple.getS();
            
            URI p = triple.getP();
            
            URI o = triple.getO();
            
            buffer.add( s, p, o, StatementEnum.Axiom );
            
        }

        // write on the database.
        buffer.flush();

        /*
         * Fill the btree with the axioms in SPO order.
         */
        {
        
            // exact fill of the root leaf.
            final int branchingFactor = axioms.size();
            
            btree = new BTree(new TemporaryRawStore(),
                    branchingFactor, UUID.randomUUID(),
                    StatementSerializer.INSTANCE);

            // SPO[] exposed by our StatementBuffer subclass.
            SPO[] stmts = ((MyStatementBuffer)buffer).stmts;
            
            for(SPO spo : stmts ) {

                btree.insert(keyBuilder.statement2Key(KeyOrder.SPO, spo),
                        spo.type.serialize());

            }
            
        }
        
    }

    /**
     * Return true iff the fully bound statement is an axiom.
     * 
     * @param db
     *            The axioms will be defined using the term identifiers for this
     *            database.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return
     */
    public boolean isAxiom(long s, long p, long o) {

        if (btree == null) {

            defineAxioms();

        }

        byte[] key = keyBuilder.statement2Key(s,p,o);
        
        if(btree.contains(key)) {
            
            return true;
            
        }
        
        return false;
        
    }

    static class MyStatementBuffer extends StatementBuffer {

        /**
         * An array of the axioms in SPO order.
         */
        SPO[] stmts;
        
        /**
         * @param database
         * @param capacity
         */
        public MyStatementBuffer(AbstractTripleStore database, int capacity) {

            super(database, capacity);
            
        }

        /**
         * Save off a copy of the axioms in SPO order on {@link #stmts}.
         */
        protected int writeSPOs(SPO[] stmts, int numStmts) {
            
            if (this.stmts == null) {

                this.stmts = new SPO[numStmts];

                System.arraycopy(stmts, 0, this.stmts, 0, numStmts);

                Arrays.sort( this.stmts, KeyOrder.SPO.getComparator() );
                
            }
            
            return super.writeSPOs(stmts, numStmts);
            
        }
        
    }
    
}
