package se.liu.ida.hefquin.federation.access.impl.reqproc;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTPBuilder;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.base.data.utils.SolutionMappingUtils;
import se.liu.ida.hefquin.base.utils.BuildInfo;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.FederationAccessException;
import se.liu.ida.hefquin.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.federation.access.SolMapRetrievalInterface;
import se.liu.ida.hefquin.federation.access.SolMapsResponse;
import se.liu.ida.hefquin.federation.access.impl.response.SolMapsResponseImpl;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SamplingSPARQLRequestProcessorImpl implements SamplingSPARQLRequestProcessor {
    protected final HttpClient httpClient;

    public SamplingSPARQLRequestProcessorImpl() {
        this(-1L);
    }

    /**
     * The given timeouts are specified in milliseconds. Any value {@literal <=} 0 means no timeout.
     */
    public SamplingSPARQLRequestProcessorImpl( final long connectionTimeout ) {
        httpClient = createHttpClient(connectionTimeout);
    }

    protected static HttpClient createHttpClient( final long connectionTimeout ) {
        final HttpClient.Builder httpClientBuilder = HttpClient.newBuilder()
                .followRedirects( HttpClient.Redirect.ALWAYS );

        if ( connectionTimeout > 0L )
            httpClientBuilder.connectTimeout( Duration.ofMillis(connectionTimeout) );

        return httpClientBuilder.build();
    }

    @Override
    public SolMapsResponse performRequest(SPARQLRequest req, SPARQLEndpoint fm) throws FederationAccessException {
        throw new UnsupportedOperationException("No sampling without budget.");
    }

    @Override
    public SolMapsResponse performRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException {
        return performRequestWithQueryExecutionHTTP(req, fm, budget);
    }

    protected SolMapsResponse performRequestWithQueryExecutionHTTP( final SPARQLRequest req,
                                                                    final SPARQLEndpoint fm,
                                                                    final Budget budget)
            throws FederationAccessException
    {
        // see https://jena.apache.org/documentation/sparql-apis/#query-execution

        final QueryExecution qe;
        try {
            qe = QueryExecutionHTTPBuilder.create()
                    .endpoint( fm.getInterface().getURL() )
                    .httpClient( httpClient )
                    .query( req.getQuery().asJenaQuery() )
                    .timeout( budget.getTimeout(), TimeUnit.MILLISECONDS )
                    .param("budget", budget.getAttempts().toString() )
                    .httpHeader("User-Agent", BuildInfo.getUserAgent() )
                    .build();
        }
        catch ( final Exception e ) {
            throw new FederationAccessException("Initiating the remote execution of a query at the SPARQL endpoint at '" + fm.getInterface().getURL() + "' caused an exception.", e, req, fm);
        }

        final ResultSet result = qe.execSelect();

        final Date requestStartTime = new Date();

        // consume the query result
        final List<SolutionMapping> solMaps = new ArrayList<>();
        try {
            while ( result.hasNext() ) {
                final QuerySolution s = result.next();
                solMaps.add( SolutionMappingUtils.createSolutionMapping(s) );
            }
        }
        catch ( final Exception ex ) {
            try { result.close(); } catch ( final Exception e ) { e.printStackTrace(); }
            throw new FederationAccessException("Consuming the query result from the SPARQL endpoint at '" + fm.getInterface().getURL() + "' caused an exception.", ex, req, fm);
        }

        result.close();

        return new SolMapsResponseImpl(solMaps, fm, req, requestStartTime);
    }
}
