package com.bigdata.rdf.sail.webapp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

public class TestEOFException967<S extends IIndexManager> extends
AbstractTestNanoSparqlClient<S> {

	public TestEOFException967() {
		super();
	}
	
	public TestEOFException967(final String name) {
		super(name);
	}
	
	public static Test suite() {
		
        return ProxySuiteHelper.suiteWhenStandalone(TestEOFException967.class,
        "test967", TestMode.quads);
	    
	}

	public void test967() throws Exception {
		
		final IIndexManager im = getIndexManager();
		
		System.err.println(im.getClass().getName());
		
		final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(100);
		final AtomicInteger namespaceCount = new AtomicInteger(0);
		final CountDownLatch latch = new CountDownLatch(2);
		final AtomicBoolean testSucceeding = new AtomicBoolean(true);

		// 1. Create namespace
		executor.submit(new Runnable() {
			@Override
			public void run() {
				final String namespace = "n" + namespaceCount.getAndIncrement();
				final Properties properties = new Properties();
				properties.put("com.bigdata.rdf.sail.namespace", namespace);
				try {
					log.info(String.format("Create namespace %s...", namespace));
					m_repo.createRepository(namespace, properties);
					log.warn(String.format("Create namespace %s done", namespace));
					latch.countDown();
				} catch (final Exception e) {
					log.error(String.format("Failed to create namespace %s:", namespace), e);
					testSucceeding.set(false);
				}
				if (testSucceeding.get())
					executor.schedule(this, 10, TimeUnit.SECONDS);
			}
		});

		// 2. Data load
		executor.submit(new Runnable() {
			@Override
			public void run() {
				String namespace = null;
				try {
					latch.await(); // Wait at least 2 created namespaces
					namespace = "n" + ThreadLocalRandom.current().nextInt(namespaceCount.get() - 1);
					final Collection<Statement> stmts = new ArrayList<>(100000);
					for (int i = 0; i < 100000; i++) {
						stmts.add(generateTriple());
					}
					log.info(String.format("Loading package into %s namespace...", namespace));
					m_repo.getRepositoryForNamespace(namespace).add(new RemoteRepository.AddOp(stmts));
					log.warn(String.format("Loading package into %s namespace done", namespace));
				} catch (final Exception e) {
					log.error(String.format("Failed to load package into namespace %s:", namespace), e);
					testSucceeding.set(false);
				}
				if (testSucceeding.get())
					executor.schedule(this, 10, TimeUnit.SECONDS);
			}
		});

		// 3. Get namespace list
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					log.info("Get namespace list...");
					m_repo.getRepositoryDescriptions().close();
					log.warn("Get namespace list done");
				} catch (final Exception e) {
					log.error("Failed to get namespace list:", e);
					testSucceeding.set(false);
				}
				if (testSucceeding.get())
					executor.schedule(this, 2, TimeUnit.SECONDS);
			}
		});

		// 4. Execute SPARQL
		executor.submit(new Runnable() {
			@Override
			public void run() {
				String namespace = null;
				try {
					latch.await(); // Wait at least 2 created namespaces
					namespace = "n" + ThreadLocalRandom.current().nextInt(namespaceCount.get() - 1);
					log.info(String.format("Execute SPARQL on %s namespace...", namespace));
					m_repo.getRepositoryForNamespace(namespace).prepareTupleQuery("SELECT * {?s ?p ?o} LIMIT 100").evaluate().close();
					log.warn(String.format("Execute SPARQL on %s namespace done", namespace));
				} catch (final Exception e) {
					log.error(String.format("Failed to execute SPARQL on %s namespace:", namespace), e);
					testSucceeding.set(false);
				}
				if (testSucceeding.get())
					executor.schedule(this, 2, TimeUnit.SECONDS);
			}
		});

		final int SECOND = 1000; // 1 second
		int DURATION = 60 * 60 * SECOND; // 1 hour
		while (DURATION > 0 && testSucceeding.get()) {
			Thread.sleep(SECOND); // Wait a while
			DURATION -= SECOND;
		}
		
		log.warn("Stopping...");
		
		executor.shutdownNow();
		executor.awaitTermination(5, TimeUnit.MINUTES);

		log.info("Cleanup namespaces...");
		for (int i = 0; i < namespaceCount.get(); i++) {
			m_repo.deleteRepository("n" + i);
		}

		assertTrue(testSucceeding.get());
	}

	private static Statement generateTriple() {
		final String	URI_PREFIX			= "http://bigdata.test/";
		return new StatementImpl(new URIImpl(URI_PREFIX + UUID.randomUUID()), new URIImpl(URI_PREFIX + UUID.randomUUID()), new URIImpl(URI_PREFIX + UUID.randomUUID()));
	}

}
