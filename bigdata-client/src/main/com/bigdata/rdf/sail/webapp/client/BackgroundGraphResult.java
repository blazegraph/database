/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.webapp.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.sparql.query.QueueCursor;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;

/**
 * Provides concurrent access to statements as they are being parsed.
 * 
 * @author James Leigh
 * 
 */
public class BackgroundGraphResult implements GraphQueryResult, Runnable,
        RDFHandler {
    private volatile boolean closed;
    private /*volatile*/ Thread parserThread; // Note: volatile not required with lock.
    final private RDFParser parser;
    final private Charset charset;
    final private InputStream in;
    final private String baseURI;
    final private CountDownLatch namespacesReady = new CountDownLatch(1);
    final private Map<String, String> namespaces = new ConcurrentHashMap<String, String>();
    final private QueueCursor<Statement> queue;
    public BackgroundGraphResult(final RDFParser parser, final InputStream in,
            final Charset charset, final String baseURI) {
        this(new QueueCursor<Statement>(10), parser, in, charset, baseURI);
    }

    public BackgroundGraphResult(final QueueCursor<Statement> queue,
            final RDFParser parser, final InputStream in, final Charset charset, final String baseURI) {
        this.queue = queue;
        this.parser = parser;
        this.in = in;
        this.charset = charset;
        this.baseURI = baseURI;
    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {
        return queue.hasNext();
    }

    @Override
    public Statement next() throws QueryEvaluationException {
        return queue.next();
    }

    @Override
    public void remove() throws QueryEvaluationException {
        queue.remove();
    }

   @Override
   public void close() throws QueryEvaluationException {
      closed = true;
      interruptParserThread();
      try {
         queue.close();
         in.close(); // close the input stream.
      } catch (IOException e) {
         throw new QueryEvaluationException(e);
      }
   }
   
   /**
    * Lock used to coordinate interrupt/clear of {@link #parserThread}. Without
    * this lock there is a data race between {@link #run()} clearing the
    * {@link #parserThread} reference and {@link #close()} interrupting the
    * {@link Thread}. It also prevents us from interrupting the {@link Thread}
    * after the flow of control leaves {@link #run()} (at which point it might
    * even be running a different task).
    * 
    * @see #1110
    */
   private final Lock lock = new ReentrantLock();

   /**
    * Set the {@link #parserThread} to the caller's {@link Thread}. This is
    * invoked from {@link #run()}.
    */
   private void setParserThread() {
      lock.lock();
      try {
         parserThread = Thread.currentThread();
      } finally {
         lock.unlock();
      }
   }

   /**
    * Interrupt the {@link #parserThread} iff it is set. This is invoked from
    * {@link #close()}. The lock prevents a data race between the interrupt
    * and the clear of the reference.
    */
   private void interruptParserThread() {
      lock.lock();
      try {
         final Thread t = parserThread;
         if (t != null) {
            t.interrupt();
         }
      } finally {
         lock.unlock();
      }
   }
   
   /**
    * Clear the reference to the parser thread. This is also invoked from
    * {@link #run()}. Once this method has been called, the flow of control has
    * left the parser and there is no longer a reason to interrupt it in
    * {@link #close()}.
    */
   private void clearParserThread() {
      lock.lock();
      try {
         parserThread = null;
      } finally {
         lock.unlock();
      }
   }
   
    @Override
    public void run() {
//        boolean completed = false;
       // Save reference to this thread.
       setParserThread();
        try {
            parser.setRDFHandler(this);
            if (charset == null) {
                parser.parse(in, baseURI);
            } else {
                parser.parse(new InputStreamReader(in, charset), baseURI);
            }
//            completed = true;
        } catch (RDFHandlerException e) {
            // parsing was cancelled or interrupted
        } catch (RDFParseException e) {
            queue.toss(e);
        } catch (IOException e) {
            queue.toss(e);
        } finally {
           clearParserThread();
            queue.done();
        }
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        // no-op
    }

    @Override
    public Map<String, String> getNamespaces() {
        try {
            namespacesReady.await();
            return namespaces;
        } catch (InterruptedException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    @Override
    public void handleComment(final String comment) throws RDFHandlerException {
        // ignore
    }

    @Override
    public void handleNamespace(final String prefix, final String uri)
            throws RDFHandlerException {
        namespaces.put(prefix, uri);
    }

    @Override
    public void handleStatement(final Statement st) throws RDFHandlerException {
        namespacesReady.countDown();
        if (closed)
            throw new RDFHandlerException("Result closed");
        try {
            queue.put(st);
        } catch (InterruptedException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        namespacesReady.countDown();
    }

}
