package com.bigdata.rdf.sparql.ast.eval;


/**
 * Test case fot date time parsing in queries when specifying dates containing a numerical time zone spec.
 * We suspected a bug, but there actually was only a problem in missing URL encoding for the '+' sign when
 * executing the query with curl. Checking in the test case nevertheless to increase confidence.
 *
 * See https://github.com/blazegraph/database/issues/107
 *
 * @author schmdtm
 */
public class TestBrokenDatetimeParsing extends AbstractDataDrivenSPARQLTestCase {

   public TestBrokenDatetimeParsing() {
   }

   /**
    * Data:
    * <http://s1a> <http://date> "2012-01-28T00:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    * <http://s1b> <http://date> "2012-01-28T00:00:00.000+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    * <http://s2a> <http://date> "2012-01-30T00:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    * <http://s2b> <http://date> "2012-01-30T00:00:00.000+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    *
    * Query:
    * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    * SELECT * WHERE {
    *   ?s <http://date> ?o
    *   FILTER(?o > "2012-01-29T00:00:00.000+00:00"^^xsd:dateTime)
    * }
    *
    * => expected to select s2a and s2b (which point to the same timestamp)
    */
   public void test_datetime_roundtripping_with_numerical_time_zone() throws Exception {

      new TestHelper(
              "datetime-roundtripping01",// testURI,
              "datetime-roundtripping01.rq",// queryFileURL
              "datetime-roundtripping.ttl",// dataFileURL
              "datetime-roundtripping.srx"// resultFileURL
      ).runTest();
   }

   /**
    * Variant of the test case with query containing the date specified in Zulu format, using 'Z'.
    * Note that this version used to work all the time, just added to avoid regressions.
    */
   public void test_datetime_roundtripping_with_zulu_time_zone() throws Exception {

      new TestHelper(
              "datetime-roundtripping02",// testURI,
              "datetime-roundtripping02.rq",// queryFileURL
              "datetime-roundtripping.ttl",// dataFileURL
              "datetime-roundtripping.srx"// resultFileURL
      ).runTest();
   }
}
