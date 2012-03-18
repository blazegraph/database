/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 18, 2012
 */

package com.bigdata.rdf.sail.tck;

import java.util.Properties;

import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A variant of the test suite using full read/write transactions.
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

        /**
         * FIXME This is a problem when using isolatable indices
         * 
         * <pre>
         * Caused by: java.lang.RuntimeException: Not isolatable: kb.lex.search
         *     at com.bigdata.journal.Tx.getIndex(Tx.java:895)
         *     at com.bigdata.journal.Journal.getIndex(Journal.java:868)
         *     at com.bigdata.journal.Journal.getIndex(Journal.java:1)
         *     at com.bigdata.relation.AbstractRelation.getIndex(AbstractRelation.java:205)
         *     at com.bigdata.relation.AbstractRelation.getIndex(AbstractRelation.java:165)
         *     at com.bigdata.search.FullTextIndex.getIndex(FullTextIndex.java:257)
         *     at com.bigdata.search.FullTextIndex.getKeyBuilder(FullTextIndex.java:764)
         *     at com.bigdata.search.DefaultAnalyzerFactory.getAnalyzer(DefaultAnalyzerFactory.java:77)
         *     at com.bigdata.search.FullTextIndex.getAnalyzer(FullTextIndex.java:752)
         *     at com.bigdata.search.FullTextIndex.getTokenStream(FullTextIndex.java:876)
         *     at com.bigdata.search.FullTextIndex.index(FullTextIndex.java:824)
         *     at com.bigdata.search.FullTextIndex.index(FullTextIndex.java:776)
         *     at com.bigdata.rdf.lexicon.BigdataRDFFullTextIndex.index(BigdataRDFFullTextIndex.java:290)
         *     at com.bigdata.rdf.lexicon.FullTextIndexWriterTask.call(FullTextIndexWriterTask.java:43)
         *     at com.bigdata.rdf.lexicon.FullTextIndexWriterTask.call(FullTextIndexWriterTask.java:1)
         *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
         *     at java.util.concurrent.FutureTask.run(FutureTask.java:138)
         *     ... 3 more
         * </pre>
         */
        props.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");

        return props;

    }

}
