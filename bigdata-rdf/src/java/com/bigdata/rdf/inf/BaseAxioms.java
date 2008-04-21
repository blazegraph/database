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
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * @author personickm
 */
abstract class BaseAxioms implements Axioms {
    
    private final long NULL = IRawTripleStore.NULL;
    
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
         * Note: you can not define the axioms here since the base class is not
         * yet fully initialized.
         */
//        defineAxioms();
        
    }

    protected void addAxiom
    ( URI s,
      URI p,
      URI o
      ) {
        
        addAxiom(s.toString(), p.toString(), o.toString());
        
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
    
    public int size() {
        
        return axioms.size();
        
    }
    
    public Set<Triple> getAxioms()
    {
        
        return axioms;
        
    }    
    
    public Set<String> getVocabulary()
    {
        
        return vocabulary;
        
    }

    public boolean isAxiom( Statement stmt )
    {
        
        Resource subj = stmt.getSubject();
        
        URI pred = stmt.getPredicate();
        
        Value obj = stmt.getObject();
        
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
        
        return vocabulary.contains( uri.toString() );
        
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

            // Note: context is ignored for axiom comparisons.
            hashCode = 
                ( s.stringValue() + "|" + 
                  p.stringValue() + "|" + 
                  o.stringValue()
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

            // Note: context is ignored for axiom comparisons.
            return 
                ( obj != null &&
                  obj instanceof Triple &&
                  s.equals( ((Triple)obj).getS() ) &&
                  p.equals( ((Triple)obj).getP() ) &&
                  o.equals( ((Triple)obj).getO() )
                  );
            
        }
        
        public int hashCode()
        {
            
            return hashCode;
            
        }
              
    }
    
    /**
     * Add the axiomatic RDF(S) triples to the store (conditional operation).
     * This is a NOP if the axioms are already defined locally (in this class).
     * If the axioms already exist in the database then this operation does not
     * result in any statements being written on the database (it will just
     * verify that the axioms exist in the database).
     */
    public void addAxioms() {

        if(btree==null) {

            defineAxioms();
            
        }
        
    }

    /**
     * Write the axioms on the database.
     * <p>
     * Note: if the terms for the axioms are already in the lexicon and the
     * axioms are already in the database then this will not write on the
     * database, but it will still result in the SPO[] containing the axioms
     * to be defined in MyStatementBuffer.
     */
    private void defineAxioms() {
        
        final int capacity = getAxioms().size();
        
        // Note: min capacity of one handles case with no axioms.
        MyStatementBuffer buffer = new MyStatementBuffer(db, Math.max(1,capacity) );

        for (Iterator<Axioms.Triple> itr = getAxioms().iterator(); itr
                .hasNext();) {

            Axioms.Triple triple = itr.next();
            
            URI s = triple.getS();
            
            URI p = triple.getP();
            
            URI o = triple.getO();
            
            buffer.add( s, p, o, null, StatementEnum.Axiom );
            
        }

        // write on the database.
        buffer.flush();

        /*
         * Fill the btree with the axioms in SPO order.
         */
        {
        
            // exact fill of the root leaf.
            final int branchingFactor = Math.max(BTree.MIN_BRANCHING_FACTOR, axioms.size() );
            
            /*
             * Note: This uses a SimpleMemoryRawStore since we never explictly
             * close the BaseAxioms class. Also, all data should be fully
             * buffered in the leaf of the btree so the btree will never touch
             * the store after it has been populated.
             * 
             * @todo use FastRDFValueCompression once API is aligned.
             */
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(branchingFactor);
            
            btree = BTree.create(new SimpleMemoryRawStore(), metadata);
            
//            btree = new BTree(new SimpleMemoryRawStore(), branchingFactor, UUID
//                    .randomUUID(), ByteArrayValueSerializer.INSTANCE);

            // SPO[] exposed by our StatementBuffer subclass.
            SPO[] stmts = ((MyStatementBuffer)buffer).stmts;
            
            if (stmts != null) {

                for (SPO spo : stmts) {

                    btree.insert(keyBuilder.statement2Key(KeyOrder.SPO, spo),
                            spo.getType().serialize());

                }

            }
            
        }
        
    }

    /**
     * Return true iff the fully bound statement is an axiom. The axioms will be
     * written on the database using {@link #addAxioms()} iff necessary.
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

        // fast rejection.
        if (s == NULL || p == NULL || o == NULL) {

            return false;
            
        }
        
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
