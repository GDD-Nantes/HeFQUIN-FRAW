package se.liu.ida.hefquin.federation.access.impl;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.NodeFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.base.query.impl.TriplePatternImpl;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.*;
import se.liu.ida.hefquin.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.federation.access.impl.reqproc.SamplingSPARQLRequestProcessor;
import se.liu.ida.hefquin.federation.access.impl.reqproc.SamplingSPARQLRequestProcessorImpl;
import se.liu.ida.hefquin.federation.access.impl.response.SolMapsResponseImpl;
import se.liu.ida.hefquin.federation.catalog.FederationDescriptionReader;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SamplingFederationAccessManagerWithBufferTest  {

    protected static ExecutorService execServiceForFedAccess;

    final TriplePattern tp = new TriplePatternImpl( NodeFactory.createVariable( "s" ),
            NodeFactory.createVariable( "p" ),
            NodeFactory.createVariable( "o" ) );
    final SPARQLRequest sparqlReq = new SPARQLRequestImpl( tp );

    final TriplePattern tp2 = new TriplePatternImpl( NodeFactory.createVariable( "s" ),
            NodeFactory.createURI( "a" ),
            NodeFactory.createVariable( "o" ) );
    final SPARQLRequest sparqlReq2 = new SPARQLRequestImpl( tp2 );

    @BeforeClass
    public static void createExecService() {
        final int numberOfThreads = 1;
        execServiceForFedAccess = Executors.newFixedThreadPool( numberOfThreads );
    }

    @AfterClass
    public static void tearDownExecService() {
        execServiceForFedAccess.shutdownNow();
        try {
            execServiceForFedAccess.awaitTermination( 500L, TimeUnit.MILLISECONDS );
        }
        catch ( final InterruptedException ex )  {
            System.err.println( "Terminating the thread pool was interrupted." );
            ex.printStackTrace();
        }
    }

    @Test
    public void issue_request_uses_buffer_when_same_request_twice() throws FederationAccessException {
        // Need a running endpoint at URL
        String URL = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor0.fr/";

        SamplingSPARQLRequestProcessor samSPARQLReqProc = new SamplingSPARQLRequestProcessorImpl();
        SamplingFederationAccessManagerWithBuffer samFedAccMan = new SamplingFederationAccessManagerWithBuffer(samSPARQLReqProc);

        Budget budget = new Budget().setAttempts(10);

        SPARQLEndpoint sparqlEp = getSparqlEndpoint(URL);

        samFedAccMan.issueRequest(sparqlReq, sparqlEp, budget);
        samFedAccMan.issueRequest(sparqlReq, sparqlEp, budget);

        Assert.assertEquals(1, samFedAccMan.getBufferHitCounter());
    }

    @Test
    public void issue_request_does_not_use_buffer_when_same_request_at_different_endpoints() throws FederationAccessException {
        // Need running endpoint at URL1 and URL2
        String URL1 = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor0.fr/";
        String URL2 = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor1.fr/";

        SamplingSPARQLRequestProcessor samSPARQLReqProc = new SamplingSPARQLRequestProcessorImpl();
        SamplingFederationAccessManagerWithBuffer samFedAccMan = new SamplingFederationAccessManagerWithBuffer(samSPARQLReqProc);

        Budget budget = new Budget().setAttempts(10);

        SPARQLEndpoint sparqlEp1 = getSparqlEndpoint(URL1);
        SPARQLEndpoint sparqlEp2 = getSparqlEndpoint(URL2);

        samFedAccMan.issueRequest(sparqlReq, sparqlEp1, budget);
        samFedAccMan.issueRequest(sparqlReq, sparqlEp2, budget);

        Assert.assertEquals(0, samFedAccMan.getBufferHitCounter());
    }

    @Test
    public void issue_request_does_not_use_buffer_when_different_request_at_same_endpoint() throws FederationAccessException {
        // Need running endpoint at URL1 and URL2
        String URL = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor0.fr/";

        SamplingSPARQLRequestProcessor samSPARQLReqProc = new SamplingSPARQLRequestProcessorImpl();
        SamplingFederationAccessManagerWithBuffer samFedAccMan = new SamplingFederationAccessManagerWithBuffer(samSPARQLReqProc);

        Budget budget = new Budget().setAttempts(10);

        SPARQLEndpoint sparqlEp = getSparqlEndpoint(URL);

        samFedAccMan.issueRequest(sparqlReq, sparqlEp, budget);
        samFedAccMan.issueRequest(sparqlReq2, sparqlEp, budget);

        Assert.assertEquals(0, samFedAccMan.getBufferHitCounter());
    }

    @NotNull
    private static SPARQLEndpoint getSparqlEndpoint(String URL) {
        SPARQLEndpoint sparqlEp = new SPARQLEndpoint(){
            @Override public SPARQLEndpointInterface getInterface() {return new SPARQLEndpointInterface() {
                @Override public boolean supportsTriplePatternRequests() {return false;}
                @Override public boolean supportsBGPRequests() {return false;}
                @Override public boolean supportsSPARQLPatternRequests() {return false;}
                @Override public boolean supportsRequest(DataRetrievalRequest req) {return false;}
                @Override public int getID() {return 0;}
                @Override public String getURL() {return URL;}
            };}
            @Override public VocabularyMapping getVocabularyMapping() {return null;}
        };
        return sparqlEp;
    }
}
