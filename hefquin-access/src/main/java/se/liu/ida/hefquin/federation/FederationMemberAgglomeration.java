package se.liu.ida.hefquin.federation;

import se.liu.ida.hefquin.federation.access.FederationMemberAgglomerationInterface;

public interface FederationMemberAgglomeration extends SPARQLEndpoint {

    @Override
    FederationMemberAgglomerationInterface getInterface();

}
