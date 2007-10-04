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
 * Created on Mar 3, 2006
 */
package com.bigdata.concurrent.schedule;

import java.util.Random;

import junit.framework.TestCase;

/**
 * Test of the {@link Schedule}.
 * 
 * @author thompsonbry
 */

public class TestSchedule extends TestCase
{

    /**
     * 
     */
    public TestSchedule() {
        super();
    }

    /**
     * @param name
     */
    public TestSchedule(String name) {
        super(name);
    }

    /**
     * Verifies that we can run a series of NOPs for a single transaction.
     */
    
    public void test_schedule_nops_001()
    {

        Schedule s = new Schedule();

        Tx tx = s.createTx("tx");

        int N = 100;
        
        for( int i=0; i<N; i++ ) {
            s.add( new Action.NOP<Tx>( tx, "NOP["+i+"]" ) );
        }

        s.run();
        
    }
    
    /**
     * Test of the {@link Schedule}runs some nops for multiple transactions.
     */

    public void test_schedule_nops_002()
    {

        Schedule s = new Schedule();

        Tx tx1 = s.createTx("tx1");

        Tx tx2 = s.createTx("tx2");

        s.add( new Action.NOP<Tx>( tx1 ) );
        s.add( new Action.NOP<Tx>( tx2 ) );
        s.add( new Action.NOP<Tx>( tx2 ) );
        s.add( new Action.NOP<Tx>( tx2 ) );
        s.add( new Action.NOP<Tx>( tx1 ) );
        s.add( new Action.NOP<Tx>( tx1 ) );

        s.run();
        
    }
    
    /**
     * Test runs N NOPs distributed across M transactions.
     */

    public void test_schedule_nops_003()
    {
        
        Random r = new Random(); 

        Schedule s = new Schedule();

        final int N = 100;
        final int M = 10;
        
        Tx[] tx = new Tx[ M ];
        
        for( int i=0; i<M; i++ ) {
            
            tx[ i ] = s.createTx("tx"+i);
        
        }

        for( int i=0; i<N; i++ ) {
        
            int index = r.nextInt( M );
            
            Tx t = tx[ index ];
            
            s.add( new Action.NOP<Tx>( t, "NOP["+s.getActionCount(t)+"](Action#"+i+")" ) );
            
        }
        
        s.run();
        
    }
    
    /**
     * Test of the {@link Schedule}in which a transaction blocks while another
     * transaction continues to execute.
     */

    public void test_schedule_block_001()
    {
        
        Schedule s = new Schedule();

        Tx tx1 = s.createTx("tx1");

        Tx tx2 = s.createTx("tx2");

        s.add( new Action.Block<Tx>( tx1 ) );
    
        s.add( new Action.NOP<Tx>( tx2, "NOP1" ) );

        s.add( new Action.NOP<Tx>( tx2, "NOP2" ) );
        
        s.run();

    }
    
    
    /**
     * Test of the {@link Schedule}in which a transaction blocks while another
     * transaction continues to execute and the interrupts the first transaction
     * which then continues to execute normally.
     */

    public void test_schedule_block_002()
    {
        
        Schedule s = new Schedule();

        Tx tx1 = s.createTx("tx1");

        Tx tx2 = s.createTx("tx2");

        s.add( new Action.Block<Tx>( tx1 ) );
    
        s.add( new Action.NOP<Tx>( tx2, "NOP1" ) );

        s.add( new Action.NOP<Tx>( tx2, "NOP2" ) );
        
        s.add( new Action.Interrupt<Tx>( tx2, tx1 ) );
        
        s.add( new Action.NOP<Tx>( tx1, "NOP3" ) );
        
        s.add( new Action.NOP<Tx>( tx1, "NOP4" ) );
        
        s.run();

    }
    
}
