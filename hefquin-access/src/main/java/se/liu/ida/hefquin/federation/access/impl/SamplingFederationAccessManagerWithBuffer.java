package se.liu.ida.hefquin.federation.access.impl;

import org.apache.commons.lang3.tuple.Pair;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.CardinalityResponse;
import se.liu.ida.hefquin.federation.access.FederationAccessException;
import se.liu.ida.hefquin.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.federation.access.SolMapsResponse;
import se.liu.ida.hefquin.federation.access.impl.reqproc.SamplingSPARQLRequestProcessor;
import se.liu.ida.hefquin.federation.access.impl.response.SolMapsResponseImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This Sampling Federation Access Manager is budgetable,and  retrieves solution mapping by sending requests
 * to endpoints with the provided budget.
 * Retrieved solution mappings are stored in a buffer specific to each request / endpoint pair.
 */
public class SamplingFederationAccessManagerWithBuffer extends SamplingFederationAccessManagerImpl {

    Map<Pair<SPARQLRequest, SPARQLEndpoint>, Queue<SolMapsResponse>> buffer;
    AtomicInteger bufferHitCounter = new AtomicInteger(0);

    public SamplingFederationAccessManagerWithBuffer(SamplingSPARQLRequestProcessor reqProcSPARQL) {
        super(reqProcSPARQL);
        buffer = new ConcurrentHashMap<>();
    }

    public SolMapsResponse _issueRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException {
        Pair<SPARQLRequest, SPARQLEndpoint> key = Pair.of(req, fm);

        if(!buffer.containsKey(key)) {
            buffer.put(key, new LinkedList<>());
        }

        if(!buffer.get(key).isEmpty()){
            bufferHitCounter.incrementAndGet();
            return buffer.get(key).poll();
        }

        SolMapsResponse solMapsResponse = requestProcessor.performRequest(req, fm, budget);
        if(solMapsResponse.getSize() == 0) {
            buffer.get(key).offer(new SolMapsResponseImpl(List.of(), fm, req, solMapsResponse.getRequestStartTime()));
        }
        else {
            solMapsResponse.getResponseData().iterator().forEachRemaining(sm ->
                    buffer.get(key).offer(new SolMapsResponseImpl(List.of(sm), fm, req, solMapsResponse.getRequestStartTime()))
            );
        }

        return buffer.get(key).poll();
    }

    /**
     *
     * @param req the request to sample
     * @param fm the federation member on which to sample the request
     * @param budget the budget information sent to the endpoint
     * @return a CompletableFuture of a solMapResponse containing exactly one solution mapping
     * @throws FederationAccessException if actually sending a request caused an exception
     */
    @Override
    public CompletableFuture<SolMapsResponse> issueRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException {
        // /!\ blocking
        return CompletableFuture.completedFuture(_issueRequest(req, fm, budget));
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int getBufferHitCounter() {
        return bufferHitCounter.get();
    }
}
