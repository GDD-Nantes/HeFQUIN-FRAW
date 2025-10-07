package se.liu.ida.hefquin.federation.access.impl.reqproc;

import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.FederationAccessException;
import se.liu.ida.hefquin.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.federation.access.SolMapRetrievalInterface;
import se.liu.ida.hefquin.federation.access.SolMapsResponse;

public interface SamplingSPARQLRequestProcessor extends SPARQLRequestProcessor {
    /**
     * Assumes that fm has a {@link SolMapRetrievalInterface}.
     */
    SolMapsResponse performRequest(SPARQLRequest req, SPARQLEndpoint fm, Budget budget) throws FederationAccessException;
}
