package com.bigdata.service;

/**
 * Run states for the {@link AbstractTransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum TxServiceRunState {

    /**
     * During startup.
     */
    Starting(0),
    /**
     * While running (aka open).
     */
    Running(1),
    /**
     * When shutting down normally.
     */
    Shutdown(2),
    /**
     * When shutting down immediately.
     */
    ShutdownNow(3),
    /**
     * When halted.
     */
    Halted(4);

    private TxServiceRunState(int val) {

        this.val = val;

    }

    final private int val;

    public int value() {

        return val;

    }

    public boolean isTransitionLegal(final TxServiceRunState newval) {

        if (this == Starting) {

            if (newval == Running)
                return true;

            if (newval == Halted)
                return true;

        } else if (this == Running) {

            if (newval == Shutdown)
                return true;

            if (newval == ShutdownNow)
                return true;

        } else if (this == Shutdown) {

            if (newval == ShutdownNow)
                return true;

            if (newval == Halted)
                return true;

        } else if (this == ShutdownNow) {

            if (newval == Halted)
                return true;

        }

        return false;

    }

}
