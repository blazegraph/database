package com.bigdata.rdf.sail.webapp.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.http.HttpEntity;
import org.eclipse.jetty.client.api.ContentProvider;

public class EntityContentProvider implements ContentProvider {

	private final HttpEntity m_entity;

	public EntityContentProvider(final HttpEntity entity) {
		m_entity = entity;
	}

	@Override
	public long getLength() {
		return m_entity.getContentLength();
	}
	
	public String getContentType() {
		return m_entity.getContentType().getValue();
	}

	@Override
	public Iterator<ByteBuffer> iterator() {
		try {
			final InputStream instr;
			
			if (m_entity.isStreaming()) {
				instr = m_entity.getContent();
			} else {
				// If Apache Entity does not stream then we need to double buffer to an output stream - not ideal
				final ByteArrayOutputStream streambuf = new ByteArrayOutputStream();
				m_entity.writeTo(streambuf);
				
				instr = new ByteArrayInputStream(streambuf.toByteArray());
			}

			return new Iterator<ByteBuffer>() {
				
				boolean eof = false;
				int bufindex = 0;

				@Override
				public boolean hasNext() {
					return !eof;
				}

				@Override
				public ByteBuffer next() {
					final byte[] buf = new byte[1024];
					try {
						final int rdlen = instr.read(buf);
						
						// System.out.println("Returning ByteBuffer[" + bufindex++ + "] length: " + rdlen);
						
						if (rdlen == -1) {
							eof = true;
							return ByteBuffer.wrap(buf, 0, 0);
						} else {
							return ByteBuffer.wrap(buf, 0, rdlen);
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		} catch (Exception e) {
			throw new RuntimeException("Unexpected", e);
		}
	}
}
