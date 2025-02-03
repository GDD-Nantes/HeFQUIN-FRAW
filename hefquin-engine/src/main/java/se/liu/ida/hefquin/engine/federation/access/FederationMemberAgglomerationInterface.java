package se.liu.ida.hefquin.engine.federation.access;

import se.liu.ida.hefquin.engine.federation.FederationMember;

import java.util.List;

// Maybe there is more to it, but right now type specification should be enough, so empty interface for now
public interface FederationMemberAgglomerationInterface extends DataRetrievalInterface {
    List<FederationMember> getFederationMembers();
}
