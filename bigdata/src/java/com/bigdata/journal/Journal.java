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

import java.io.IOException;
import java.util.Properties;

/**
 * <p>
 * An append-only persistence capable data structure supporting atomic commit
 * and transactions. Writes are logically appended to the journal to minimize
 * disk head movement.
 * </p>
 */
public class Journal extends AbstractJournal {
    
    /**
     * Create or open a journal.
     * 
     * @param properties The properties as defined by {@link Options}.
     * 
     * @throws IOException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     */
    public Journal(Properties properties) throws IOException {
           
        super(properties);
    }

}
