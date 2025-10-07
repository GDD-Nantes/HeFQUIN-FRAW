package se.liu.ida.hefquin.federation.access;

import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.query.SPARQLQuery;
import se.liu.ida.hefquin.base.utils.StatsProvider;
import se.liu.ida.hefquin.federation.BRTPFServer;
import se.liu.ida.hefquin.federation.Neo4jServer;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.TPFServer;

import java.util.concurrent.CompletableFuture;

public interface SamplingFederationAccessManager extends FederationAccessManager
{
	CompletableFuture<SolMapsResponse> issueRequest( SPARQLRequest req, SPARQLEndpoint fm, Budget budget ) throws FederationAccessException;
	/**
	 * Requests the cardinality of the result of the given request.
	 *
	 * Assumes that the given request contains a {@link SPARQLGraphPattern}
	 * rather than a full {@link SPARQLQuery}. If it does not, then this
	 * method throws an {@link IllegalArgumentException}.
	 */
	CompletableFuture<CardinalityResponse> issueCardinalityRequest( SPARQLRequest req, SPARQLEndpoint fm, Budget budget ) throws FederationAccessException;

	@Override
	FederationAccessStats getStats();

	/**
	 * Shuts down all thread pools associated with this federation access manager.
	 */
	void shutdown();
}
