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
 * Created on Oct 24, 2006
 */

package com.bigdata.journal;

import junit.framework.TestCase;

/**
 * FIXME This is a work in progress for state-based validation. There are
 * several things that are different from my preconceptions, including that some
 * aspects of state appear to be tx local with the same persistent state visible
 * to all active transactions (module whatever object replication synchrony
 * issues). I need to work through this some more (the test below is not
 * functional yet) and think through the relationship between this approach and
 * the notions that I have for handling link set membership and clustered index
 * membership changes with high concurrency. One of the key differences is that
 * validation examines transaction local state and the commit then updates the
 * global state and validation is (I think) required even if only one
 * transaction has written on an object. Plus, this notion of validation is in
 * terms of the objects API (credit and debit in this case) rather than in terms
 * of writes of opaque state that are then unpacked when a write-write conflict
 * is detected.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAccount extends TestCase {

    /**
     * 
     */
    public TestAccount() {
    }

    /**
     * @param arg0
     */
    public TestAccount(String arg0) {
        super(arg0);
    }

    /**
     * An implementation of a bank account data type used to test state-based
     * validation. The basic data type just carries the account balance. An
     * instance of that data type may be wrapped up in transactional state (low,
     * high, and change). A transactional instance knows how to validate against
     * the basic data type and on commit it updates the basic data type.
     * 
     * @see http://www.cs.brown.edu/~mph/Herlihy90a/p96-herlihy.pdf, section 5.2
     *      page 112.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Account {
      
        /**
         * The account's persistent state (in cents).
         */
        long bal = 0;

    }
    
    public static class TxAccount {

        final Account account;
        
        public TxAccount(Account account) {
            
            assert account != null;
            
            this.account = account;
            
        }
        
        /**
         * The observed lower bound on the current balance (initially zero).
         */
        long low = 0;
        
        /**
         * The observed upper bound on the current balance (initially infinity).
         */
        long high = Long.MAX_VALUE;

        /**
         * The transaction's net change to the balance (initially zero).
         */
        long change = 0;
        
        public void credit( long cents ) {
            
            change = change + cents;
            
        }
        
        public void debit( long cents ) throws OverdraftException {

            if( account.bal + change >= cents ) {
                
                low = Math.max(low, cents - change);

                change = change - cents;
                
            } else {

                /*
                 * overdraft.
                 */
                
                high = Math.min( high, cents - change );
                
                throw new OverdraftException();
                
            }
            
        }

        public String toString() {
            
            return "bal="+account.bal+", low="+low+", high="+high+", change="+change;

        }

        public boolean validate()  {
        
            System.err.println("validate: "+toString());
            
            /*
             * @todo This really needs to synchronize on the account balance for
             * validation, or perhaps obtain a lock for validation and commit.
             * Otherwise concurrent transactions that commit together could
             * resolve account.bal into two distinct values within the
             * comparison below.
             */
            
            if( low <= account.bal && account.bal < high ) {
                
                // valid.
                
                return true;
                
            }
            
            return false;
            
        }
        
        public void commit() {
            
            // @todo This operation needs to be atomic, which it is not.
            
            account.bal = account.bal + change;
            
        }
        
        public static class OverdraftException extends RuntimeException {

            private static final long serialVersionUID = 1L;
            
        }
        
    }

    public void test_Schedule01() {
        
        Account a = new Account();
        TxAccount p = new TxAccount(a);
        TxAccount q = new TxAccount(a);

        assertEquals("bal", 0, a.bal);

        assertEquals("low", 0, p.low );
        assertEquals("high", Long.MAX_VALUE, p.high );
        assertEquals("change", 0, p.change );
        
        assertEquals("low", 0, q.low );
        assertEquals("high", Long.MAX_VALUE, q.high );
        assertEquals("change", 0, q.change );
        
        p.credit(5);

        assertEquals("low", 0, p.low );
        assertEquals("high", Long.MAX_VALUE, p.high );
        assertEquals("change", 5, p.change);
        
        q.credit(6);
        
        assertEquals("low", 0, q.low );
        assertEquals("high", Long.MAX_VALUE, q.high );
        assertEquals("change", 6, q.change);
        
        assertEquals("bal", 0, a.bal);
        assertTrue(p.validate());
        p.commit();
        assertEquals("bal", 5, a.bal);
        
        q.debit(10);
        
        assertEquals("low", 0, q.low );
        assertEquals("high", Long.MAX_VALUE, q.high );
        assertEquals("change", 10, q.change);
        
        assertEquals("bal", 5, a.bal);
        q.validate();
        q.commit();
        assertEquals("bal", 1, a.bal);
        
    }
    
}
