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
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

import java.util.Random;

/**
 * Test suite for basic {@link Journal} operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo tests of creating a new journal, including with bad properties.
 * 
 * @todo tests of opening an existing journal, including with incomplete writes
 *       of a root block.
 * 
 * @todo tests when the journal is very large. This is NOT the normal use case
 *       for bigdata, but one option for an embedded database. However, even an
 *       embedded database would normally use a read-optimized database segment
 *       paired with the journal.
 * 
 * @todo Do stress test with writes, reads, and deletes.
 * 
 * @todo tests of the exclusive lock mechanism during startup/shutdown (the
 *       advisory file locking mechanism). This is not used for the
 *       memory-mapped mode, but it is used for both "Direct" and "Disk" modes.
 * 
 * @todo test for correct detection of a "full" journal.
 * 
 * @todo test ability to extend the journal.
 * 
 * @todo test ability to compact and truncate the journal. Compaction moves
 *       slots from the end of the journal to fill holes earlier in the journal.
 *       Truncation chops off the tail. Compaction is done in order to
 *       facilitate truncation for a journal whose size requirements have
 *       decreased based on observed load characteristics.
 * 
 * @todo write tests for correct migration of committed records to a database.
 * 
 * @todo write tests for correct logical deletion of records no longer readable
 *       by any active transaction (they have been since updated or deleted by a
 *       committed transaction _and_ there is no transaction running that has a
 *       view of a historical consistent state in which that record version is
 *       visible).
 */
public class TestJournal extends ProxyTestCase {

    Random r = new Random();
        
    public TestJournal() {
    }

    public TestJournal(String arg0) {
        super(arg0);
    }

    public void test_something() {
        
        fail("write tests");
        
    }
    
}
