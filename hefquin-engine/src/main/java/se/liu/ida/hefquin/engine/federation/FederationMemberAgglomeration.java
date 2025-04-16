package se.liu.ida.hefquin.engine.federation;

import se.liu.ida.hefquin.engine.federation.access.FederationMemberAgglomerationInterface;

public interface FederationMemberAgglomeration extends SPARQLEndpoint {

    @Override
    FederationMemberAgglomerationInterface getInterface();

}
