package se.liu.ida.hefquin.federation.access.impl;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.base.query.impl.BGPImpl;
import se.liu.ida.hefquin.base.query.impl.TriplePatternImpl;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.*;
import se.liu.ida.hefquin.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.federation.access.impl.reqproc.SamplingSPARQLRequestProcessor;
import se.liu.ida.hefquin.federation.access.impl.reqproc.SamplingSPARQLRequestProcessorImpl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SamplingFederationAccessManagerTest {

    protected static ExecutorService execServiceForFedAccess;

    final TriplePattern tp = new TriplePatternImpl( NodeFactory.createVariable( "s" ),
            NodeFactory.createVariable( "p" ),
            NodeFactory.createVariable( "o" ) );
    final SPARQLRequest sparqlReq = new SPARQLRequestImpl( tp );

    final TriplePattern tp2 = new TriplePatternImpl( NodeFactory.createVariable( "s" ),
            NodeFactory.createURI( "p1" ),
            NodeFactory.createVariable( "o1" ) );
    final TriplePattern tp3 = new TriplePatternImpl( NodeFactory.createVariable( "o1" ),
            NodeFactory.createVariable( "p2" ),
            NodeFactory.createVariable( "o2" ) );

    final Triple triple1 = Triple.create(NodeFactory.createVariable( "s" ),
            NodeFactory.createURI( "p1" ),
            NodeFactory.createVariable( "o1" ));
    final Triple triple2 = Triple.create(NodeFactory.createVariable( "o1" ),
            NodeFactory.createURI( "p2" ),
            NodeFactory.createVariable( "o2" ));
    final Triple triple3 = Triple.create(NodeFactory.createVariable( "o2" ),
            NodeFactory.createURI( "p3" ),
            NodeFactory.createVariable( "o3" ));

    final BasicPattern bp2triples = BasicPattern.wrap(List.of(triple1, triple2));
    final BasicPattern bp3triples = BasicPattern.wrap(List.of(triple1, triple2, triple3));

    final SPARQLRequest sparqlReq2 = new SPARQLRequestImpl( new BGPImpl( bp2triples ) );
    final SPARQLRequest sparqlReq3 = new SPARQLRequestImpl( new BGPImpl( bp3triples ) );

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

    // TODO: add more tests : different bgps, configs, and on local services

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    public void issue_request_spo_retrieves_results_according_to_attempt_budget(int attempts) throws FederationAccessException, ExecutionException, InterruptedException {
        // Need a running endpoint at URL
        String URL = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor0.fr/";

        Budget budget = new Budget().setAttempts(attempts);

        SamplingSPARQLRequestProcessor samSPARQLReqProc = new SamplingSPARQLRequestProcessorImpl();
        SamplingFederationAccessManager samFedAccMan = new SamplingFederationAccessManagerImpl(samSPARQLReqProc);

        SPARQLEndpoint sparqlEp = getSparqlEndpoint(URL);

        SolMapsResponse solMapsResponse = samFedAccMan.issueRequest(sparqlReq, sparqlEp, budget).get();

        Assertions.assertEquals(attempts, solMapsResponse.getSize());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    public void issue_request_bgp_3_triples_retrieves_results_according_to_attempt_budget(int attempts) throws FederationAccessException, ExecutionException, InterruptedException {
        // Need a running endpoint at URL
        String URL = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor0.fr/";

        Budget budget = new Budget().setAttempts(attempts);

        SamplingSPARQLRequestProcessor samSPARQLReqProc = new SamplingSPARQLRequestProcessorImpl();
        SamplingFederationAccessManager samFedAccMan = new SamplingFederationAccessManagerImpl(samSPARQLReqProc);

        SPARQLEndpoint sparqlEp = getSparqlEndpoint(URL);

        SolMapsResponse solMapsResponse = samFedAccMan.issueRequest(sparqlReq, sparqlEp, budget).get();

        Assertions.assertTrue(attempts >= solMapsResponse.getSize());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    public void issue_request_retrieves_results_according_to_limit_budget(int limit) throws FederationAccessException, ExecutionException, InterruptedException {
        // Need a running endpoint at URL
        String URL = "http://localhost:3030/fedshop200/raw?default-graph-uri=http://www.vendor0.fr/";

        Budget budget = new Budget().setAttempts(limit);


        SamplingSPARQLRequestProcessor samSPARQLReqProc = new SamplingSPARQLRequestProcessorImpl();
        SamplingFederationAccessManager samFedAccMan = new SamplingFederationAccessManagerImpl(samSPARQLReqProc);

        SPARQLEndpoint sparqlEp = getSparqlEndpoint(URL);

        SolMapsResponse solMapsResponse = samFedAccMan.issueRequest(sparqlReq, sparqlEp, budget).get();

        Assertions.assertTrue(limit >= solMapsResponse.getSize());
    }

    @NotNull
    private static SPARQLEndpoint getSparqlEndpoint(String URL) {
        return new SPARQLEndpoint(){
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
    }
}
