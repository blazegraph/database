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
 * Created on Feb 27, 2006
 */
package com.bigdata.journal;

/**
 * Isolation levels for a transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum IsolationEnum {

    /**
     * A read-only transaction that will read any data successfully
     * committed on the database (the view provided by the transaction does
     * not remain valid as of the transaction start time but evolves as
     * concurrent transactions commit) (level 0).
     */
    ReadCommitted(0),

    /**
     * A fully isolated read-only transaction (level 1).
     */
    ReadOnly(1),

    /**
     * A fully isolated read-write transaction (level 2).
     */
    ReadWrite(2);

    private final int level;

    private IsolationEnum(int level) {

        this.level = level;
        
    }

    /**
     * The integer code for the isolation level.
     */
    public int getLevel() {
        
        return level;
        
    }
    
    /**
     * Convert an integer isolation level into an {@link IsolationEnum}.
     * 
     * @param level
     *            The isolation level.
     *            
     * @return The corresponding enum value.
     */
    public IsolationEnum get(int level) {

        switch (level) {
        case 0:
            return ReadCommitted;
        case 1:
            return ReadOnly;
        case 2:
            return ReadWrite;
        default:
            throw new IllegalArgumentException("level=" + level);
        }

    }
    
}
