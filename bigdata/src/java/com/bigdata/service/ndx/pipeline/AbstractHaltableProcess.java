/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 15, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.util.InnerCause;

/**
 * Abstract base class for tasks whose processing may be halted asynchronously.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractHaltableProcess {

    protected final transient static Logger log = Logger
            .getLogger(AbstractHaltableProcess.class);

    /**
     * Volatile flag is set <code>true</code> if the process should halt.
     */
    volatile transient private boolean halt = false;

    /**
     * The first cause as set by {@link #halt(Throwable)}.
     * <p>
     * Note: {@link #halt} uses
     * {@link AtomicReference#compareAndSet(Object, Object)} and specifies
     * <code>null</code> as the expected value to ensure that only the first
     * cause is recorded by this field.
     */
    final transient private AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>(
            null);

    /**
     * Return unless processing has been halted.
     * 
     * @throws RuntimeException
     *             wrapping the {@link #firstCause} iff processing has been
     *             halted.
     * 
     * @see #halt(Throwable)
     */
    final protected void halted() {

        if (halt) {

            throw new RuntimeException(firstCause.get());

        }

    }

    /**
     * Indicate that processing should halt. This method is written defensively
     * and will not throw anything. The caller is responsible for throwing the
     * cause out of their own context.
     * 
     * @param cause
     *            The cause.
     * 
     * @return The argument.
     */
    final protected Throwable halt(final Throwable cause) {

        halt = true;

        final boolean isFirstCause = firstCause.compareAndSet(
                null/* expect */, cause);

        if (log.isEnabledFor(Level.WARN))

            try {

                if (!InnerCause.isInnerCause(cause, InterruptedException.class)
                        && !InnerCause.isInnerCause(cause,
                                CancellationException.class)
                        && !InnerCause.isInnerCause(cause,
                                ClosedByInterruptException.class)
                        && !InnerCause.isInnerCause(cause,
                                RejectedExecutionException.class)
                        && !InnerCause.isInnerCause(cause,
                                BufferClosedException.class)) {

                    /*
                     * This logs all unexpected causes, not just the first one
                     * to be reported for this join task.
                     * 
                     * Note: The master will log the firstCause that it receives
                     * as an error.
                     */

                    log.warn(this + " : isFirstCause=" + isFirstCause + " : "
                            + cause.getLocalizedMessage(), cause);

                }

            } catch (Throwable ex) {

                // error in logging system - ignore.

            }

        return cause;

    }

}
