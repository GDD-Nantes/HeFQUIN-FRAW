package se.liu.ida.hefquin.engine.federation.access.impl.iface;

import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalRequest;
import se.liu.ida.hefquin.engine.federation.access.FederationMemberAgglomerationInterface;
import se.liu.ida.hefquin.engine.federation.access.impl.DataRetrievalInterfaceBase;

import java.util.List;

public class FederationMemberAgglomerationInterfaceImpl extends DataRetrievalInterfaceBase implements FederationMemberAgglomerationInterface {

    List<FederationMember> members;

    public FederationMemberAgglomerationInterfaceImpl(List<FederationMember> members) {
        this.members = members;
    }

    @Override
    public boolean supportsTriplePatternRequests() {
        // idk
        return true;
    }

    @Override
    public boolean supportsBGPRequests() {
        // idk
        return true;
    }

    @Override
    public boolean supportsSPARQLPatternRequests() {
        // idk
        return true;
    }

    @Override
    public boolean supportsRequest(DataRetrievalRequest req) {
        // idk
        return true;
    }

    @Override
    public List<FederationMember> getFederationMembers() {
        return members;
    }
}
