package se.liu.ida.hefquin.engine.federation.access.impl.iface;

import org.apache.commons.collections4.MultiValuedMap;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.FederationMemberAgglomeration;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalRequest;
import se.liu.ida.hefquin.engine.federation.access.FederationMemberAgglomerationInterface;
import se.liu.ida.hefquin.engine.federation.access.SPARQLEndpointInterface;
import se.liu.ida.hefquin.engine.federation.access.impl.DataRetrievalInterfaceBase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class FederationMemberAgglomerationInterfaceImpl extends DataRetrievalInterfaceBase implements FederationMemberAgglomerationInterface {

    Set<FederationMember> members;

    public FederationMemberAgglomerationInterfaceImpl(Set<FederationMember> members) {
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
}
