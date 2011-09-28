/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 20, 2011
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedTupleQuery;

import com.bigdata.rdf.sparql.ast.QueryType;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataParsedTupleQuery extends ParsedTupleQuery implements
        IBigdataParsedQuery {

    private final QueryType queryType;
    private final Properties queryHints;

    /**
     * @param tupleExpr
     */
    public BigdataParsedTupleQuery(final TupleExpr tupleExpr,
            final QueryType queryType,
            final Properties queryHints) {

        super(tupleExpr);

        this.queryType = queryType;
        
        this.queryHints = queryHints;

    }

    public QueryType getQueryType() {
        
        return queryType;
        
    }
    
    public Properties getQueryHints() {

        return queryHints;

    }

}
