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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * @author personickm
 */
abstract class BaseAxioms implements Axioms {
    
    Set<Triple> axioms = new HashSet<Triple>();
    
    Set<String> vocabulary = new HashSet<String>();
    
    protected BaseAxioms()
    {
        
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
     * <p>
     * Note: The termIds are defined with respect to the backing triple store
     * since the axioms will be copied into the store when the closure is
     * complete.
     * 
     * @param database
     *            The store to which the axioms will be added.
     */
    public void addAxioms(AbstractTripleStore database) {
        
        StatementBuffer buffer = new StatementBuffer(database, getAxioms()
                .size());

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
        
    }

}
