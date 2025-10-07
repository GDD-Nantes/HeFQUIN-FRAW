package se.liu.ida.hefquin.federation.access.impl;

import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.federation.BRTPFServer;
import se.liu.ida.hefquin.federation.Neo4jServer;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.TPFServer;
import se.liu.ida.hefquin.federation.access.*;
import se.liu.ida.hefquin.federation.access.impl.reqproc.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class SamplingFederationAccessManagerImpl implements SamplingFederationAccessManager {

    final SamplingSPARQLRequestProcessor requestProcessor;
    final private static Budget DEFAULT_BUDGET = new Budget().setRemoteAttempts(10);

    // stats
    protected AtomicLong counterSPARQLRequests  = new AtomicLong(0L);

    public SamplingFederationAccessManagerImpl(final SamplingSPARQLRequestProcessor reqProcSPARQL) {
        this.requestProcessor = reqProcSPARQL;
    }

    @Override
    public CompletableFuture<SolMapsResponse> issueRequest(SPARQLRequest req, SPARQLEndpoint fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<TPFResponse> issueRequest(TPFRequest req, TPFServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<TPFResponse> issueRequest(TPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<TPFResponse> issueRequest(BRTPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<RecordsResponse> issueRequest(Neo4jRequest req, Neo4jServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(SPARQLRequest req, SPARQLEndpoint fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(TPFRequest req, TPFServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(TPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(BRTPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new UnsupportedOperationException("Not sampling without budget.");
    }

    @Override
    public CompletableFuture<SolMapsResponse> issueRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException {
        // /!\ blocking
        return CompletableFuture.completedFuture(requestProcessor.performRequest(req, fm, budget));
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException {
        return null;
    }

    @Override
    public FederationAccessStats getStats() {
        return null;
    }

    @Override
    public void resetStats() {
        counterSPARQLRequests.set(0);
    }

    @Override
    public void shutdown() {

    }
}
