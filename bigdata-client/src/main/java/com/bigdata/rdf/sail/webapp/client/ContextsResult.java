/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008-2009.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.webapp.client;

import info.aduna.iteration.IterationWrapper;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.http.client.QueueCursor;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;
import org.openrdf.model.Resource;

/**
 * Provides concurrent access to contexts results as they are being parsed.
 */
public class ContextsResult extends IterationWrapper<Resource, QueryEvaluationException> implements
			Runnable {

	final private Object closeLock = new Object();

	private volatile boolean closed;

	private Thread parserThread;

	final private SAXParser parser;

	final private InputStream in;

	final private QueueCursor<Resource> queue;

public ContextsResult(final InputStream in) 
		throws ParserConfigurationException, SAXException {

	// TODO Why is the capacity so small? Why not 100 or more?
		this(new QueueCursor<Resource>(10/* capacity */), in);

	}

public ContextsResult(final QueueCursor<Resource> queue,
final InputStream in) throws ParserConfigurationException, SAXException {
super(queue);
		this.queue = queue;
		this.in = in;
		this.parser = SAXParserFactory.newInstance().newSAXParser();
	}

@Override
	public void handleClose() throws QueryEvaluationException {
		synchronized (closeLock) {
			if (!closed) {
				closed = true;
				if (parserThread != null) {
					parserThread.interrupt();
				}
				if (in != null) {
					try {
						in.close();
					} catch (IOException e) {
						throw new QueryEvaluationException(e);
					}
				}
			}
		}
	}

@Override
	public void run() {
		synchronized (closeLock) {
			if (closed) {
				// Result was closed before it was run.
				// Need to unblock latch
				return;
			}
			parserThread = Thread.currentThread();
		}

		try {
			/*
			 * Run the parser, pumping events into this class (which is its own
			 * listener).
			 */
			parser.parse(in, new DefaultHandler2(){

			   @Override
			public void startElement(final String uri,
			final String localName, final String qName,
			final Attributes attributes) {

			if ("context".equals(qName))
							try {
								queue.put((Resource) new URIImpl(attributes.getValue("uri")));
							} catch (InterruptedException e) {
								queue.toss(e);
							}

			}
			
			});
		} catch (IOException e) {
			queue.toss(e);
		} catch (NoClassDefFoundError e) {
			queue.toss(new QueryResultParseException(e));
		} catch (SAXException e) {
			queue.toss(e);
		} finally {
			synchronized (closeLock) {
				parserThread = null;
			}
			queue.done();
		}
	}

}
