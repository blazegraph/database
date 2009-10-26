/*
 * Copyright SYSTAP, LLC 2006-2009.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

/**
 * Unit tests for named graphs. Specify
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code> to
 * run this test suite.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class QuadsTestCase extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(QuadsTestCase.class);
    
    /**
     * 
     */
    public QuadsTestCase() {
    }

    /**
     * @param arg0
     */
    public QuadsTestCase(String arg0) {
        super(arg0);
    }

    protected BindingSet createBindingSet(final Binding... bindings) {
        final QueryBindingSet bindingSet = new QueryBindingSet();
        if (bindings != null) {
            for (Binding b : bindings) {
                bindingSet.addBinding(b);
            }
        }
        return bindingSet;
    }
    
    protected void compare(final TupleQueryResult result,
            final Collection<BindingSet> answer)
            throws QueryEvaluationException {

        final Collection<BindingSet> extraResults = new LinkedList<BindingSet>();
        Collection<BindingSet> missingResults = new LinkedList<BindingSet>();

        int resultCount = 0;
        int nmatched = 0;
        while (result.hasNext()) {
            BindingSet bindingSet = result.next();
            resultCount++;
            boolean match = false;
            if(log.isInfoEnabled())
                log.info(bindingSet);
            Iterator<BindingSet> it = answer.iterator();
            while (it.hasNext()) {
                if (it.next().equals(bindingSet)) {
                    it.remove();
                    match = true;
                    nmatched++;
                    break;
                }
            }
            if (match == false) {
                extraResults.add(bindingSet);
            }
        }
        missingResults = answer;

        for (BindingSet bs : extraResults) {
            if (log.isInfoEnabled()) {
                log.info("extra result: " + bs);
            }
        }
        
        for (BindingSet bs : missingResults) {
            if (log.isInfoEnabled()) {
                log.info("missing result: " + bs);
            }
        }
        
        if (!extraResults.isEmpty() || !missingResults.isEmpty()) {
            fail("matchedResults=" + nmatched + ", extraResults="
                    + extraResults.size() + ", missingResults="
                    + missingResults.size());
        }
        
    }
}
