package se.liu.ida.hefquin.engine.federation;

import se.liu.ida.hefquin.engine.federation.access.FederationMemberAgglomerationInterface;
import se.liu.ida.hefquin.engine.federation.access.SPARQLEndpointInterface;

public interface FederationMemberAgglomeration extends FederationMember {

    @Override
    FederationMemberAgglomerationInterface getInterface();

}
