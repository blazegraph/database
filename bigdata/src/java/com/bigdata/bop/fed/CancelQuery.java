package com.bigdata.bop.fed;

import com.bigdata.bop.engine.AbstractRunningQuery;

/**
 * {@link Runnable} will halt the query, interrupting any operators which are
 * currently running for that query.
 */
class CancelQuery implements Runnable {

    private final AbstractRunningQuery q;

    private final Throwable cause;

    /**
     * 
     * @param q
     *            The query.
     * @param cause
     *            The cause (optional). When not give, the normal termination
     *            semantics apply.
     */
    public CancelQuery(final AbstractRunningQuery q, final Throwable cause) {

        if (q == null)
            throw new IllegalArgumentException();

        this.q = q;

        this.cause = cause; // MAY be null

    }

    public void run() {

        if (cause == null)
            q.halt();
        else
            q.halt(cause);

    }

}
