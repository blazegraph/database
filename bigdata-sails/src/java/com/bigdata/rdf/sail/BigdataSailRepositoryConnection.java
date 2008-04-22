package com.bigdata.rdf.sail;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;

public class BigdataSailRepositoryConnection extends SailRepositoryConnection {
    public BigdataSailRepositoryConnection(BigdataSailRepository repository,
            SailConnection sailConnection) {
        super(repository, sailConnection);
    }
    
    @Override
    public SailGraphQuery prepareGraphQuery(QueryLanguage ql, String queryString, String baseURI)
        throws MalformedQueryException
    {
        ParsedGraphQuery parsedQuery = QueryParserUtil.parseGraphQuery(ql, queryString, baseURI);
        return new BigdataSailGraphQuery(parsedQuery, this);
    }
}
