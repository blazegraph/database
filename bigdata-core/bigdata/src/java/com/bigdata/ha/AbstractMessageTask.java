package com.bigdata.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.IHAMessage;
import com.bigdata.quorum.ServiceLookup;

/**
 * Helper class submits the RMI for a PREPARE, COMMIT, or ABORT message. This is
 * used to execute the different requests in parallel on a local executor
 * service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract class AbstractMessageTask<S extends Remote, T, M extends IHAMessage>
        implements Callable<T> {

    private final ServiceLookup<S> serviceLookup;
    private final UUID serviceId;
    protected final M msg;

    public AbstractMessageTask(final ServiceLookup<S> serviceLookup,
            final UUID serviceId, final M msg) {

        this.serviceLookup = serviceLookup;

        this.serviceId = serviceId;

        this.msg = msg;

    }

    @Override
    final public T call() throws Exception {

        /*
         * Note: This code MAY be interrupted at any point if the Future for the
         * task is cancelled. If it is interrupted during the RMI, then the
         * expectation is that the NIO will be interrupted in a timely manner
         * throwing back some sort of IOException indicating the asynchronous
         * close of the IO channel or cancel of the RMI.
         */

        // Resolve proxy for remote service.
        final S service = serviceLookup.getService(serviceId);

        // RMI.
        final Future<T> ft = doRMI(service);

        try {

            /*
             * Await the inner Future for the RMI.
             * 
             * Note: In fact, this is a ThickFuture so it is already done by the
             * time the RMI returns.
             */

            return ft.get();

        } finally {

            ft.cancel(true/* mayInterruptIfRunning */);

        }

    }

    /**
     * Invoke the specific RMI using the message supplied to the constructor.
     * 
     * @param service
     *            The service (resolved from the service {@link UUID} supplied
     *            to the constructor).
     * 
     * @return The result of submitting that RMI to the remote service.
     * 
     * @throws IOException
     *             if there is a problem with the RMI.
     */
    abstract protected Future<T> doRMI(final S service) throws IOException;

}