package com.bigdata.service.proxy;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * {@link Serializable} class wraps a {@link RemoteFuture} delegating
 * methods through to the {@link Future} on the remote service while
 * masquerading {@link IOException}s so that we can implement the
 * {@link Future} API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public class ClientFuture<T> implements Future<T>, Serializable {
    
    protected static final transient Logger log = Logger
            .getLogger(ClientFuture.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = -910518634373204705L;
    
    private final RemoteFuture<T> proxy;

    /**
     * 
     * @param proxy
     *            A proxy for the {@link RemoteFuture}.
     */
    public ClientFuture(final RemoteFuture<T> proxy) {

        if (proxy == null)
            throw new IllegalArgumentException();

        this.proxy = proxy;

    }

    /**
     * Note: I have observed problems where an attempt to cancel a remote future
     * fails when DGC is enabled. The stack trace looks like:
     * 
     * <pre>
     * java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.rmi.NoSuchObjectException: no such object in table
     *         at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:232)
     *         at java.util.concurrent.FutureTask.get(FutureTask.java:91)
     *         at com.bigdata.service.proxy.RemoteFutureImpl.get(RemoteFutureImpl.java:48)
     *         at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     *         at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
     *         at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
     *         at java.lang.reflect.Method.invoke(Method.java:597)
     *         at net.jini.jeri.BasicInvocationDispatcher.invoke(BasicInvocationDispatcher.java:1126)
     *         at net.jini.jeri.BasicInvocationDispatcher.dispatch(BasicInvocationDispatcher.java:608)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$6.run(ObjectTable.java:597)
     *         at net.jini.export.ServerContext.doWithServerContext(ServerContext.java:103)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$Target.dispatch0(ObjectTable.java:595)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$Target.access$700(ObjectTable.java:212)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$5.run(ObjectTable.java:568)
     *         at java.security.AccessController.doPrivileged(Native Method)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$Target.dispatch(ObjectTable.java:565)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$Target.dispatch(ObjectTable.java:540)
     *         at com.sun.jini.jeri.internal.runtime.ObjectTable$RD.dispatch(ObjectTable.java:778)
     *         at net.jini.jeri.connection.ServerConnectionManager$Dispatcher.dispatch(ServerConnectionManager.java:148)
     *         at com.sun.jini.jeri.internal.mux.MuxServer$2.run(MuxServer.java:244)
     *         at java.security.AccessController.doPrivileged(Native Method)
     *         at com.sun.jini.jeri.internal.mux.MuxServer$1.run(MuxServer.java:241)
     *         at com.sun.jini.thread.ThreadPool$Worker.run(ThreadPool.java:136)
     *         at java.lang.Thread.run(Thread.java:619)
     *         at com.sun.jini.jeri.internal.runtime.Util.__________EXCEPTION_RECEIVED_FROM_SERVER__________(Util.java:108)
     *         at com.sun.jini.jeri.internal.runtime.Util.exceptionReceivedFromServer(Util.java:101)
     *         at net.jini.jeri.BasicInvocationHandler.unmarshalThrow(BasicInvocationHandler.java:1303)
     *         at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:832)
     *         at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *         at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *         at $Proxy8.get(Unknown Source)
     *         at com.bigdata.service.proxy.ClientFuture.get(ClientFuture.java:67)
     *         at com.bigdata.relation.rule.eval.pipeline.JoinMasterTask.awaitAll(JoinMasterTask.java:528)
     *         at com.bigdata.relation.rule.eval.pipeline.JoinMasterTask.call(JoinMasterTask.java:378)
     *         at com.bigdata.relation.rule.eval.pipeline.JoinMasterTask.call(JoinMasterTask.java:236)
     *         at com.bigdata.relation.rule.eval.AbstractStepTask.runOne(AbstractStepTask.java:331)
     *         at com.bigdata.relation.rule.eval.MutationTask.call(MutationTask.java:113)
     *         at com.bigdata.relation.rule.eval.MutationTask.call(MutationTask.java:55)
     *         at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
     *         at java.util.concurrent.FutureTask.run(FutureTask.java:138)
     *         at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:885)
     *         at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
     *         at java.lang.Thread.run(Thread.java:619)
     * Caused by: java.lang.RuntimeException: java.rmi.NoSuchObjectException: no such object in table
     *         at com.bigdata.service.proxy.ClientFuture.cancel(ClientFuture.java:48)
     *         at com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask.cancelSinks(DistributedJoinTask.java:956)
     *         at com.bigdata.relation.rule.eval.pipeline.JoinTask.call(JoinTask.java:511)
     *         at com.bigdata.relation.rule.eval.pipeline.JoinTask.call(JoinTask.java:128)
     *         ... 5 more
     * Caused by: java.rmi.NoSuchObjectException: no such object in table
     *         at net.jini.jeri.BasicObjectEndpoint.executeCall(BasicObjectEndpoint.java:420)
     *         at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:806)
     *         at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *         at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *         at $Proxy9.cancel(Unknown Source)
     *         at com.bigdata.service.proxy.ClientFuture.cancel(ClientFuture.java:46)
     *         ... 8 more
     * </pre>
     * 
     * This appears to be an RMI/DGC bug where the remote future is somehow
     * garbage collected even though this class is holding a reference to its
     * {@link RemoteFuture} proxy.
     * <p>
     * Since the remote future no longer exists we can assume that it is no
     * longer running. Therefore, I have modified this method to log a warning
     * and return <code>false</code> when this exception is thrown.
     * <p>
     * Note: I have also seen this problem where the stack trace involves
     * <code>java.rmi.ConnectException</code>.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6181943
     * @see http://archives.java.sun.com/cgi-bin/wa?A2=ind0509&L=rmi-users&P=617
     * @see http://archives.java.sun.com/cgi-bin/wa?A2=ind0512&L=rmi-users&P=3747
     * @see http://archives.java.sun.com/cgi-bin/wa?A2=ind0601&L=rmi-users&P=1985
     */
    public boolean cancel(final boolean mayInterruptIfRunning) {

        try {

            return proxy.cancel(mayInterruptIfRunning);
            
        } catch (java.rmi.ConnectException ex) {

            /*
             * Log a warning.
             */
            if (log.isEnabledFor(Level.WARN)) {

                log.warn(ex.getLocalizedMessage());

            }
            
            /*
             * Return false since not provably cancelled in response to this
             * request.
             */
            
            return false;

        } catch (java.rmi.NoSuchObjectException ex) {
            
            /*
             * Log a warning.
             */
            if (log.isEnabledFor(Level.WARN)) {

                log.warn(ex.getLocalizedMessage());

            }
            
            /*
             * Return false since not provably cancelled in response to this
             * request.
             */
            
            return false;
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }

    }

    public T get() throws InterruptedException, ExecutionException {

        try {
            return proxy.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {

        try {
            return proxy.get(timeout, unit);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isCancelled() {

        try {
            return proxy.isCancelled();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isDone() {

        try {
            return proxy.isDone();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    
}