package com.bigdata.rdf.sail.model;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class TestJsonModelSerialization extends TestCase {
	
	public final static String rQueryJson = "[{\"extQueryId\":\"10\",\"queryUuid\":\"2392a97d-64e7-4c63-bde0-da4fdb49ae9d\",\"begin\":1433272158065380000,\"isUpdateQuery\":false,\"cancelled\":false,\"elapsedTimeNS\":4177445000},{\"extQueryId\":\"11\",\"queryUuid\":\"e38c5441-758b-4805-ae95-05a46e279297\",\"begin\":1433272159648011000,\"isUpdateQuery\":false,\"cancelled\":false,\"elapsedTimeNS\":2594833000}]";

	/**
	 * Test the serialization from  Java Objects to JSON
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	@org.junit.Test
	public void testSerialToJson() throws JsonGenerationException, JsonMappingException, IOException {
		final List<RunningQuery> rQueriesOrig = deserialize(rQueryJson);
		final List<RunningQuery> rQueriesDeserial;
		
		final StringWriter sw = new StringWriter();
		
		JsonHelper.writeRunningQueryList(sw, rQueriesOrig);

		final String jsonResult = sw.toString();
		
		assert(jsonResult != null);
		
		rQueriesDeserial = deserialize(jsonResult);
	
		final Iterator<RunningQuery> it1 = rQueriesOrig.iterator();
		final Iterator<RunningQuery> it2 = rQueriesDeserial.iterator();
		
		//Can't compare directly as the elapsed time will be different
		
		while(it1.hasNext() && it2.hasNext())
		{
			final RunningQuery r1 = it1.next();
			final RunningQuery r2 = it2.next();
			
			assertEquals(r1.getBegin(),r2.getBegin());
			assertEquals(r1.getQueryUuid(),r2.getQueryUuid());
			assertEquals(r1.getExtQueryId(),r2.getExtQueryId());
		}
	
		//They should have the same number of elements
		assert(!it1.hasNext());
		assert(!it2.hasNext());
		
	}
	
	/**
	 * Test the serialization from JSON to Java Objects
	 */
	@org.junit.Test
	public void testSerializedFromJson() {
	
		final List<RunningQuery> rQueries = deserialize(rQueryJson);

		assert(rQueries != null );
		
		final Iterator<RunningQuery> iter = rQueries.iterator();
		
		int i = 0;
		
		assert(iter.hasNext());
		
		while(iter.hasNext()) {
		
			final RunningQuery r = iter.next();
			i++;
		}
		
		assertEquals(i,2);
	}
	
	/**
	 * Convenience method to deserialize JSON.
	 * @return
	 */
	private List<RunningQuery> deserialize(String str){
		
		//FIXME:  Resolve when upgrading to newer version of Jackson that
		//supports writers
		final StringBufferInputStream reader = new StringBufferInputStream(str);

		try {
			return JsonHelper.readRunningQueryList(reader);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
		
	}

}
