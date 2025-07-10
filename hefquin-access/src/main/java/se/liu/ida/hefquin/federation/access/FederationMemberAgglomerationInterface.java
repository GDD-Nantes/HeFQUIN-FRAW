package se.liu.ida.hefquin.federation.access;


import se.liu.ida.hefquin.federation.FederationMember;

import java.util.List;

// Maybe there is more to it, but right now type specification should be enough, so empty interface for now
public interface FederationMemberAgglomerationInterface extends SPARQLEndpointInterface {
    List<FederationMember> getMembers();

}
