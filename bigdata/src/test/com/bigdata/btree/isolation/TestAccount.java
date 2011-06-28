/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 24, 2006
 */

package com.bigdata.btree.isolation;

import junit.framework.TestCase;

/**
 * This test case demonstrates a state-based validation technique described in
 * http://www.cs.brown.edu/~mph/Herlihy90a/p96-herlihy.pdf for a "bank account"
 * data type.
 * 
 * @todo There are several things that are different about this approach from my
 *       preconceptions.<br>
 *       First, there is a distinct between stable state (the account balance)
 *       and transaction local state (the low, high, and change for that account
 *       within the transaction).<br>
 *       This notion of validation is in terms of the objects API (credit and
 *       debit in this case) rather than in terms of writes of opaque state that
 *       are then unpacked when a write-write conflict is detected.<br>
 *       In the atomic commit protocol, validation examines transaction local
 *       state as well as the global state and updates the global state
 *       atomically iff validation succeeds.<br>
 *       This does not appear to depend on the notion of version counters to 
 *       trigger state-based 
 *       This raises lots of questions. I need to think through how a
 *       transaction containing multiple objects could be modeled, how this
 *       relates to what is already implemented, and the relationship between
 *       this approach and the notions that I have for handling link set
 *       membership and clustered index membership changes with high
 *       concurrency.
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
    
    /**
     * A transactional view of an {@link Account}. Each transaction has its own
     * transaction local state (low, high, and change). All operations within a
     * transaction are applied to the {@link TxAccount}. If the transaction
     * validates and commits, then the net <i>change</i> in the balance due to
     * the operations on the transaction is applied to the {@link Account}
     * balance.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
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

        /**
         * Credit the account.
         * 
         * @param cents
         *            The amount.
         */
        public void credit( long cents ) {
            
            change = change + cents;
            
        }

        /**
         * Debit the account.
         * 
         * @param cents
         *            The amount.
         * @throws OverdraftException
         *             iff an overdraft must result.
         */
        public void debit( long cents ) throws OverdraftException {
// 5 + 6 > 10
            if( account.bal + change >= cents ) {
// 4 = max( 0, 10 - 6 )                
                low = Math.max(low, cents - change);
// -4 = 6 - 10
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

        /**
         * <p>
         * Validate the transaction against the current account balance.
         * </p>
         * <p>
         * Note: Validation is currently invoked from within the {@link #commit()},
         * which handles atomicity with respect to the account balance.
         * </p>
         */
        public boolean validate() {

            System.err.println("validate: " + toString());

            if (low <= account.bal && account.bal < high) {

                // valid.

                return true;

            }

            return false;

        }
        
        /**
         * <p>
         * Validate against the current account balance and commit the change to
         * the account.
         * </p>
         * <p>
         * Note: This validate + commit operation needs to be atomic. That is
         * achieved here by synchronizing on the {@link #account}. However that
         * technique will not work in a distributed system.
         * </p>
         */
        public void commit() {

            synchronized (account) {

                if (!validate()) {

                    throw new RuntimeException("Validation error.");

                }

                account.bal = account.bal + change;

            }
            
        }
        
        public static class OverdraftException extends RuntimeException {

            private static final long serialVersionUID = 1L;
            
        }
        
    }

    /**
     * <p>
     * Runs a schedule and verifies the intermediate and stable states for an
     * {@link Account}.
     * </p>
     * <p>
     * The schedule is from page 101 of
     * http://www.cs.brown.edu/~mph/Herlihy90a/p96-herlihy.pdf. This schedule
     * interleaves two transactions, P and Q. There is an initial balance of $0.
     * There is a $5 credit on P followed by a $6 credit on Q. P then validates
     * and commits (validation occurs during the commit protocol), with a
     * resulting stable balance of $5. A $10 debit is then made on Q and Q
     * validates and commits (again, validation is part of the commit). The
     * final stable balance is $1.
     * </p>
     * 
     * <pre>
     *         a Credit($5)/Ok( ) P
     *         a Credit($6)/Ok( ) Q
     *         a Commit P
     *         a Debit($lO)/Ok( ) Q
     *         a Commit Q
     * </pre>
     */
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
//        assertTrue(p.validate());
        p.commit();
        assertEquals("bal", 5, a.bal);
        
        q.debit(10);
        
        assertEquals("low", 4, q.low );
        assertEquals("high", Long.MAX_VALUE, q.high );
        assertEquals("change", -4, q.change);
        
        assertEquals("bal", 5, a.bal);
//       assertTrue(q.validate());
        q.commit();
        assertEquals("bal", 1, a.bal);
        
    }
    
}
