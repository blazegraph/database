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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Abstract base class for an action executed by a transaction in accordance
 * with a global {@link Schedule}.
 * 
 * @author thompsonbry
 * 
 * @param T
 *            The type of the object that models a "transaction" or other kind
 *            of operation in a multi-programming system.
 */ 
abstract public class Action<T extends Tx> implements Runnable
 {

    public static final Logger log = Logger.getLogger(Action.class);

    final private T tx;

    final private String name;

    final private List<Condition> preConditions = new LinkedList<Condition>();

    final private List<Condition> postConditions = new LinkedList<Condition>();

    /**
     * True iff the action is expected to block.
     */
    final boolean blocks;

    public T getTx() {
        return tx;
    }

    public String getName() {
        return name;
    }

    public Action(T tx, String name) {
        this(tx, name, false);
    }

    public Action(T tx, String name, boolean blocks) {
        this.tx = tx;
        this.name = name;
        this.blocks = blocks;
    }

    public String toString() {
        return getName();
    }

    public Action addPreCondition(Condition condition) {
        if (condition == null) {
            throw new IllegalArgumentException();
        }
        preConditions.add(condition);
        return this;
    }

    public Action addPostCondition(Condition condition) {
        if (condition == null) {
            throw new IllegalArgumentException();
        }
        postConditions.add(condition);
        return this;
    }

    public void runPreConditions() {
        Iterator itr = preConditions.iterator();
        while (itr.hasNext()) {
            Condition condition = (Condition) itr.next();
            condition.check(getTx());
        }
    }

    public void runPostConditions() {
        Iterator itr = postConditions.iterator();
        while (itr.hasNext()) {
            Condition condition = (Condition) itr.next();
            condition.check(getTx());
        }
    }

    /**
     * An action which does nothing.
     * 
     * @author thompsonbry
     */
    public static class NOP<T extends Tx> extends Action<T> {
        public NOP(T tx) {
            this(tx, "NOP");
        }

        public NOP(T tx, String name) {
            super(tx, name);
        }

        public void run() {
            // NOP.
        }
    }

    /**
     * Block the transaction.
     * 
     * @author thompsonbry
     */
    public static class Block<T extends Tx> extends Action<T> {
        public Block(T tx) {
            this(tx, "block(" + tx + ")");
        }

        public Block(T tx, String name) {
            super(tx, name, true);
        }

        public void run() {
            // Block.
            T tx = getTx();
            try {
                log.info("wait: tx=" + tx);
                while(true) {
                    Thread.sleep(Long.MAX_VALUE);
                }
//                tx.wait();
//                log.info("resume: tx=" + tx);
            } catch (InterruptedException ex) {
                log.info("interrupted: tx=" + tx, ex);
            }
            //                while( true ) {
            //                    try {
            //                        Thread.sleep(100L);
            //                    }
            //                    catch( InterruptedException ex ) {
            //                    }
            //                }
        }
    }

    /**
     * Send an interrupt to a transaction.
     * 
     * @author thompsonbry
     */
    public static class Interrupt<T extends Tx> extends Action<T> {
        private final T targetTx;

        public Interrupt(T tx, T targetTx) {
            this(tx, targetTx, "interrupt(" + tx + "->" + targetTx + ")");
        }

        public Interrupt(T tx, T targetTx, String name) {
            super(tx, name);
            if (targetTx == null) {
                throw new IllegalArgumentException();
            }
            //                if( tx == targetTx ) {
            //                    throw new IllegalArgumentException("targetTx MUST be different transaction");
            //                }
            this.targetTx = targetTx;
        }

        public void run() {
            log.info("sending interrupt: tx=" + getTx()
                    + ", targetTx=" + targetTx);
            targetTx.interrupt();
        }
    }

    //      public static interface Read extends Action {}
    //      public static interface Write extends Action {}

    /**
     * Helper class defines an action used to commit a transaction.
     * 
     * @author thompsonbry
     */
    public static class Commit<T extends Tx> extends Action<T> {
        public Commit(T tx) {
            super(tx, "commit(" + tx + ")");
        }

        public void run() {
            getTx().done = true;
        }
    }

}
