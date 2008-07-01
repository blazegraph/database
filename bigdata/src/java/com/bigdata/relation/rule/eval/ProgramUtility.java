/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation.rule.eval;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.LocalDataServiceFederation.LocalDataServiceImpl;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProgramUtility {
    
    protected static Logger log = Logger.getLogger(ProgramUtility.class);

    private final IRelationLocator relationLocator;
    private final long timestamp;

    public ProgramUtility(IJoinNexus joinNexus) {
        
        this(joinNexus.getRelationLocator(), joinNexus.getTimestamp());
        
    }

    public ProgramUtility(IRelationLocator relationLocator, long timestamp) {
        
        if (relationLocator == null)
            throw new IllegalArgumentException();
        
        this.relationLocator = relationLocator;

        this.timestamp = timestamp;
        
    }
    
    /**
     * The set of distinct relations identified by the various rules.
     */
    public Set<IRelationName> getRelationNames(IProgram program) {

        Set<IRelationName> c = new HashSet<IRelationName>();
        
        getRelationNames(program, c);

        if(log.isDebugEnabled()) {
            
            log.debug("Found "+c.size()+" relations, program="+program.getName());
            
        }

        return c;
        
    }
    
    private void getRelationNames(IProgram p, Set<IRelationName> c) {

        if (p.isRule()) {

            IRule r = (IRule) p;

            c.add(r.getHead().getRelationName());

        } else {
            
            final Iterator<IProgram> itr = p.steps();

            while (itr.hasNext()) {

                getRelationNames(itr.next(), c);

            }

        }

    }
    
    /**
     * Locate all relation identifers and resolve them to their relations.
     * 
     * @throws RuntimeException
     *             if any relation can not be resolved.
     * @throws RuntimeException
     *             if any relation is not local.
     */
    public Map<IRelationName, IRelation> getRelations(IProgram program) {

        if (program == null)
            throw new IllegalArgumentException();

        final Map<IRelationName, IRelation> c = new HashMap<IRelationName, IRelation>();

        getRelations(program, c);

        if (log.isDebugEnabled()) {

            log.debug("Located " + c.size() + " relations, program="
                    + program.getName());

        }

        return c;

    }

    @SuppressWarnings("unchecked")
    private void getRelations(IProgram p, Map<IRelationName, IRelation> c) {

        if (p.isRule()) {

            final IRule r = (IRule) p;

            final IRelationName relationName = r.getHead().getRelationName();

            if (!c.containsKey(relationName)) {

                c.put(relationName, relationLocator.getRelation(relationName,
                        timestamp));
                
            }

        } else {
            
            final Iterator<IProgram> itr = p.steps();

            while (itr.hasNext()) {

                getRelations(itr.next(), c);

            }

        }

    }
    
    /**
     * Returns the names of the indices maintained by the relations.
     * 
     * @param c
     *            A collection of {@link IRelation}s.
     * 
     * @return The names of the indices maintained by those relations.
     */
    @SuppressWarnings("unchecked")
    public Set<String> getIndexNames(Collection<IRelation> c) {

        if (c == null)
            throw new IllegalArgumentException();

        if (c.isEmpty())
            return Collections.EMPTY_SET;

        final Set<String> set = new HashSet<String>();
        
        final Iterator<IRelation> itr = c.iterator();
        
        while(itr.hasNext()) {
            
            final IRelation relation = itr.next();
            
            set.addAll(relation.getIndexNames());
            
        }

        return set;
        
    }

    /**
     * Return the {@link IBigdataFederation} that is capable of resolving all
     * non-local indices used by the {@link IRelation}s in the program -or-
     * <code>null</code> if all indices are resolved by local resources
     * outside of {@link IBigdataFederation} control.
     * <p>
     * Note: If the {@link IBigdataFederation} is completely local to the
     * machine on which the program will run and any indices outside of the
     * federation's control are also local to that same machine, then the
     * federation reference will be returned and the local resources will be
     * resolved.
     * <p>
     * Note: If any index is hosted by a scale-out {@link IBigdataFederation}
     * and there are any machine local resources that are not resolvable by a
     * remote host for some part of the program then an exception will be
     * thrown.
     * <p>
     * Note: If the returned {@link IBigdataFederation} is a
     * {@link LocalDataServiceFederation} then optimizations are possible by
     * running program as an {@link AbstractTask} within the
     * {@link LocalDataServiceImpl}, including the case where there is access
     * to local resources.
     * 
     * @param program
     *            The program.
     * 
     * @return The {@link IBigdataFederation} -or- <code>null</code> if the
     *         indices are all hosted by <strong>local</strong> resources
     *         outside of {@link IBigdataFederation} control, such as a
     *         {@link Journal} or {@link TemporaryStore}.
     * 
     * @throws IllegalArgumentException
     *             if <i>program</i> is <code>null</code>
     * 
     * @throws IllegalArgumentException
     *             if <i>program</i> is empty (not an {@link IRule} and having
     *             no {@link IProgram#steps()}).
     * 
     * @throws IllegalStateException
     *             if some of the indices are hosted on local resources and
     *             {@link IBigdataFederation#isDistributed()} is
     *             <code>true</code> (this implies that those resources MAY be
     *             unreachable from at least some of the {@link DataService}s
     *             in the federation).
     * 
     * @throws IllegalStateException
     *             if there are relations belonging to different
     *             {@link IBigdataFederation}s.
     */
    public IBigdataFederation getFederation(IProgram program) {

        if (program == null)
            throw new IllegalArgumentException();

        if(!program.isRule() && program.stepCount()==0)
            throw new IllegalArgumentException();

        // set of distinct relations.
        final Map<IRelationName,IRelation> relations = getRelations(program);

        if(relations.isEmpty()) {
            
            // Note: this is either an empty program or a bug.
            throw new IllegalArgumentException();
            
        }
        
        // set of distinct index managers (IBigdataFederation, TemporaryStore, Journal, etc).
        final Set<IIndexManager> indexManagers = new HashSet<IIndexManager>();

        IBigdataFederation fed = null;
        
        final Set<IIndexManager> localResources = new HashSet<IIndexManager>();
        
        final Set<IBigdataFederation> feds = new HashSet<IBigdataFederation>();
        
        for(IRelation relation : relations.values()) {
            
            final IIndexManager tmp = relation.getIndexManager();
            
            indexManagers.add( tmp );
            
            if(tmp instanceof IBigdataFederation) {
                
                feds.add( (IBigdataFederation)tmp );
               
                if(fed == null) {
                
                    fed = (IBigdataFederation)tmp;
                    
                }
                
            } else {
            
                localResources.add(tmp);
                
            }
            
        }

        assert !indexManagers.isEmpty();
        
        if(fed == null) {
            
            /*
             * No federation, so all resources must be local.
             */
            
            assert ! localResources.isEmpty();

            // Note: [null] indicates non-federation based operation.
            return null;
            
        }

        if (feds.size() > 2) {
            
            throw new IllegalStateException(
                    "Program uses more than one federation: #feds="
                            + feds.size());
            
        }

        if(!localResources.isEmpty() && fed.isDistributed()) {
            
            throw new IllegalStateException("Program uses local resources but federation is distributed.");
            
        }
        
        return fed;
        
    }

}
