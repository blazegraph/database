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

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
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
    private volatile Thread parserThread;
    final private RDFParser parser;
    final private Charset charset;
    final private InputStream in;
    final private String baseURI;
    final private CountDownLatch namespacesReady = new CountDownLatch(1);
    final private Map<String, String> namespaces = new ConcurrentHashMap<String, String>();
    final private QueueCursor<Statement> queue;
    final private HttpEntity entity;

    public BackgroundGraphResult(final RDFParser parser, final InputStream in,
            final Charset charset, final String baseURI, final HttpEntity entity) {
        this(new QueueCursor<Statement>(10), parser, in, charset, baseURI,
                entity);
    }

    public BackgroundGraphResult(final QueueCursor<Statement> queue,
            final RDFParser parser, final InputStream in, final Charset charset, final String baseURI,
            final HttpEntity entity) {
        this.queue = queue;
        this.parser = parser;
        this.in = in;
        this.charset = charset;
        this.baseURI = baseURI;
        this.entity = entity;
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
        if (parserThread != null) {
            parserThread.interrupt();
        }
        try {
            queue.close();
            in.close();
        } catch (IOException e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public void run() {
        boolean completed = false;
        parserThread = Thread.currentThread();
        try {
            parser.setRDFHandler(this);
            if (charset == null) {
                parser.parse(in, baseURI);
            } else {
                parser.parse(new InputStreamReader(in, charset), baseURI);
            }
            EntityUtils.consume(entity);
            completed = true;
        } catch (RDFHandlerException e) {
            // parsing was cancelled or interrupted
        } catch (RDFParseException e) {
            queue.toss(e);
        } catch (IOException e) {
            queue.toss(e);
        } finally {
            parserThread = null;
            queue.done();
            if (!completed) {
                try {
                    EntityUtils.consume(entity);
                } catch (IOException ex) { }
            }
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
