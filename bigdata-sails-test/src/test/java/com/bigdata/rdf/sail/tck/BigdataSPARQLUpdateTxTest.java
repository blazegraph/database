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
 * Created on Mar 18, 2012
 */

package com.bigdata.rdf.sail.tck;

import java.util.Properties;

import com.bigdata.rdf.sail.BigdataSail.Options;

/**
 * A variant of the test suite using full read/write transactions.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531" > SPARQL
 *      UPDATE for NAMED SOLUTION SETS </a>
 *      
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSPARQLUpdateTxTest extends BigdataSPARQLUpdateTest {

    /**
     * 
     */
    public BigdataSPARQLUpdateTxTest() {
    }

    @Override
    protected Properties getProperties() {

        final Properties props = super.getProperties();

        props.setProperty(Options.ISOLATABLE_INDICES, "true");

        return props;

    }
    
    @Override
    public void testAutoCommitHandling() throws Exception {
        // NOP
        // disabling autocommit test for BigdataSailRWTxConnection, which is backed 
        // by a read/write transaction and does not support begin/end nested semantics
    }

}
