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
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Provides backward chaining for property collection on owl:sameAs.
 * <p>
 * Note: 
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 *
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */ 
public class BackchainOwlSameAsPropertiesIterator implements ISPOIterator {
    
    private ISPOIterator src;
    private long sameAs;
    private Iterator<SPO> spoIterator;
    
    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments that would have resulted from
     * owl:sameAs {2,3}.
     * 
     * @param src
     *            The source iterator. {@link #nextChunk()} will sort statements
     *            into the {@link KeyOrder} reported by this iterator (as long
     *            as the {@link KeyOrder} is non-<code>null</code>).
     * @param s
     *            The subject of the triple pattern.
     * @param p
     *            The predicate of the triple pattern.
     * @param o
     *            The object of the triple pattern.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesIterator(ISPOIterator src, long s, long p,
            long o, AbstractTripleStore db, final long sameAs) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (db == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
        this.sameAs = sameAs;
        
        if ( s != NULL && p != NULL && o != NULL ) {
            accessSPO(s,p,o,db);
        }
        else
        if ( s != NULL && p != NULL && o == NULL ) {
            accessSP(s,p,db);
        }
        else
        if ( s != NULL && p == NULL && o != NULL ) {
            accessSO(s,o,db);
        }
        else
        if ( s != NULL && p == NULL && o == NULL ) {
            accessS(s,db);
        }
        else
        if ( s == NULL && p != NULL && o != NULL ) {
            accessPO(p,o,db);
        }
        else
        if ( s == NULL && p != NULL && o == NULL ) {
            accessP(p,db);
        }
        else
        if ( s == NULL && p == NULL && o != NULL ) {
            accessO(o,db);
        }
        else
        if ( s == NULL && p == NULL && o == NULL ) {
            accessAll(db);
        }
        
    }
    
    public KeyOrder getKeyOrder() 
    {
        return src.getKeyOrder();
    }

    public boolean hasNext() 
    {
        return spoIterator.hasNext();
    }

    public SPO next() 
    {
        return spoIterator.next();
    }

    public SPO[] nextChunk() 
    {
        
        final int chunkSize = 10000;

        SPO[] s = new SPO[chunkSize];

        int n = 0;
        
        while(spoIterator.hasNext() && n < chunkSize ) {
            
            s[n++] = src.next();
            
        }
        
        SPO[] stmts = new SPO[n];
        
        // copy so that stmts[] is dense.
        System.arraycopy(s, 0, stmts, 0, n);
        
        return stmts;
        
    }

    public SPO[] nextChunk(KeyOrder keyOrder) 
    {
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();
        
        if (src.getKeyOrder() != keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        return stmts;
        
    }

    public void close() 
    {
        // noop
    }

    public void remove() 
    {
        throw new RuntimeException("not implemented");
    }

    /**
     * accessS and accessSP are the same - we don't know O
     * 
     * @param s
     * @param db
     */
    public void accessS(long s,AbstractTripleStore db)
    {
        accessSP(s,NULL,db);
    }

    public void accessSP(long s,long p,AbstractTripleStore db)
    {
        
        // let TreeSet enforce uniqueness
        Set<SPO> spos = new TreeSet<SPO>();
        
        { // first collect the explicit properties from the source iterator
            while(src.hasNext()) {
                spos.add(src.next());
            }
            src.close();
        }
        
        { // now collect the properties from ? sameAs s
            // get all of s's sames
            ISPOIterator samesIt = db.getAccessPath(s, sameAs, NULL).iterator();
            while(samesIt.hasNext()) {
                long same = samesIt.next().o;
                // ignore sameAs self
                if(s==same) {
                    continue;
                }
                // attach all of the same's properties to s
                ISPOIterator propsIt = db.getAccessPath(same, p, NULL).iterator();
                while(propsIt.hasNext()) {
                    SPO prop = propsIt.next();
                    // ignore sameAs properties 
                    if(prop.p==sameAs) {
                        continue;
                    }
                    // attach the p and o to the original s
                    spos.add(new SPO(s, prop.p, prop.o,
                        StatementEnum.Inferred));
                }
            }
        }
        
        { // now collect new property values for ? sameAs o
            Collection<SPO> buffer = new LinkedList<SPO>();
            Iterator<SPO> spoIt = spos.iterator();
            while(spoIt.hasNext()) {
                SPO spo = spoIt.next();
                // ignore sameAs properties 
                if(spo.p==sameAs) {
                    continue;
                }
                // get all of o's sames
                ISPOIterator samesIt = db.getAccessPath(spo.o, sameAs, NULL).iterator();
                while(samesIt.hasNext()) {
                    long same = samesIt.next().o;
                    // ignore sameAs self
                    if(spo.o==same) {
                        continue;
                    }
                    // attach the new o to the original s and p
                    buffer.add(new SPO(spo.s, spo.p, same,
                        StatementEnum.Inferred));
                }
            }
            spos.addAll(buffer);
        }
        
        SPO[] array = spos.toArray(new SPO[spos.size()]);
        Arrays.sort(array,0,array.length,src.getKeyOrder().getComparator());
        this.spoIterator = Arrays.asList(array).iterator();
        
    }

    /**
     * accessO and accessPO are the same - we don't know S
     * 
     * @param o
     * @param db
     */
    public void accessO(long o,AbstractTripleStore db)
    {
        accessPO(NULL,o,db);
    }

    public void accessPO(long p,long o,AbstractTripleStore db)
    {
        
        // let TreeSet enforce uniqueness
        Set<SPO> spos = new TreeSet<SPO>();
        
        { // first collect the explicit reverse properties from the source iterator
            while(src.hasNext()) {
                spos.add(src.next());
            }
            src.close();
        }
        
        { // now collect the reverse properties from ? sameAs o
            // get all of o's sames
            ISPOIterator samesIt = db.getAccessPath(o, sameAs, NULL).iterator();
            while(samesIt.hasNext()) {
                long same = samesIt.next().o;
                // ignore sameAs self
                if(o==same) {
                    continue;
                }
                // attach all of the same's reverse properties to o
                ISPOIterator reversePropsIt = db.getAccessPath(NULL, p, same).iterator();
                while(reversePropsIt.hasNext()) {
                    SPO reverseProp = reversePropsIt.next();
                    // ignore sameAs properties 
                    if(reverseProp.p==sameAs) {
                        continue;
                    }
                    // attach the s and p to the original o
                    spos.add(new SPO(reverseProp.s, reverseProp.p, o,
                        StatementEnum.Inferred));
                }
            }
        }
        
        { // now collect new reverse property values for ? sameAs s
            Collection<SPO> buffer = new LinkedList<SPO>();
            Iterator<SPO> spoIt = spos.iterator();
            while(spoIt.hasNext()) {
                SPO spo = spoIt.next();
                // ignore sameAs properties 
                if(spo.p==sameAs) {
                    continue;
                }
                // get all of s's sames
                ISPOIterator samesIt = db.getAccessPath(spo.s, sameAs, NULL).iterator();
                while(samesIt.hasNext()) {
                    long same = samesIt.next().o;
                    // ignore sameAs self
                    if(spo.s==same) {
                        continue;
                    }
                    // attach the new s to the original p and o
                    buffer.add(new SPO(same, spo.p, spo.o,
                        StatementEnum.Inferred));
                }
            }
            spos.addAll(buffer);
        }
        
        SPO[] array = spos.toArray(new SPO[spos.size()]);
        Arrays.sort(array,0,array.length,src.getKeyOrder().getComparator());
        this.spoIterator = Arrays.asList(array).iterator();
        
    }
    
    /**
     * accessSO and accessSPO are the same - we know S and O
     * 
     * @param s
     * @param o
     * @param db
     */
    private void accessSO(long s,long o,AbstractTripleStore db)
    {
        accessSPO(s,NULL,o,db);
    }

    public void accessSPO(long s,long p,long o,AbstractTripleStore db)
    {
        
        // let TreeSet enforce uniqueness
        Set<SPO> spos = new TreeSet<SPO>();
        
        { // first collect the explicit links from the source iterator
            while(src.hasNext()) {
                spos.add(src.next());
            }
            src.close();
        }
        
        // collect up the links between {s,? sameAs s} X {o,? sameAs o}
        Set<Long> sAndSames = getSelfAndSames(s,db);
        Set<Long> oAndSames = getSelfAndSames(o,db);
        for(long s1 : sAndSames) {
            for(long o1 : oAndSames) {
                ISPOIterator it = db.getAccessPath(s1,p,o1).iterator();
                while(it.hasNext()) {
                    long p1 = it.next().p;
                    // ignore sameAs properties 
                    if(p1==sameAs) {
                        continue;
                    }
                    spos.add(new SPO(s, p1, o,
                        StatementEnum.Inferred));
                }
            }
        }
        
        SPO[] array = spos.toArray(new SPO[spos.size()]);
        Arrays.sort(array,0,array.length,src.getKeyOrder().getComparator());
        this.spoIterator = Arrays.asList(array).iterator();
        
    }

    /**
     * accessP and accessAll are the same - we don't know S or O
     * 
     * @param db
     */
    private void accessAll(AbstractTripleStore db)
    {
        accessP(NULL,db);
    }

    public void accessP(long p,AbstractTripleStore db)
    {

        // let TreeSet enforce uniqueness
        Set<SPO> spos = new TreeSet<SPO>();

        while(src.hasNext()) {
            SPO spo = src.next();
            spos.add(spo);
        }
        src.close();
        
        // for each spo, link {? sameAs s} with {? sameAs o} using p
        Collection<SPO> buffer = new LinkedList<SPO>();
        for(SPO spo:spos) {
            // ignore sameAs properties 
            if(spo.p==sameAs) {
                continue;
            }
            Set<Long> sAndSames = getSelfAndSames(spo.s,db);
            Set<Long> oAndSames = getSelfAndSames(spo.o,db);
            for(long s1 : sAndSames) {
                for(long o1 : oAndSames) {
                    buffer.add(new SPO(s1, spo.p, o1,
                        StatementEnum.Inferred));
                }
            }
        }
        spos.addAll(buffer);
        
        SPO[] array = spos.toArray(new SPO[spos.size()]);
        Arrays.sort(array,0,array.length,src.getKeyOrder().getComparator());
        this.spoIterator = Arrays.asList(array).iterator();

    }
    
    private Set<Long> getSelfAndSames(long id,AbstractTripleStore db)
    {
        Set<Long> selfAndSames = getSames(id,db);
        // add the self
        selfAndSames.add(id);
        return selfAndSames;
    }

    private Set<Long> getSames(long id,AbstractTripleStore db)
    {
        Set<Long> sames = new TreeSet<Long>();
        // collect up the sames
        ISPOIterator it = db.getAccessPath(id,sameAs,NULL).iterator();
        while(it.hasNext()) {
            sames.add(it.next().o);
        }
        return sames;
    }

}
