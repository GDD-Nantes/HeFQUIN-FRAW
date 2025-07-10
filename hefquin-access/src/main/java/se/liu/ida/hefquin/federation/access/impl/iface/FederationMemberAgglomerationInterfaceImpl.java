package se.liu.ida.hefquin.federation.access.impl.iface;

import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.access.DataRetrievalRequest;
import se.liu.ida.hefquin.federation.access.FederationMemberAgglomerationInterface;
import se.liu.ida.hefquin.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.federation.access.impl.DataRetrievalInterfaceBase;

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
        return req instanceof SPARQLRequest;
    }

    @Override
    public String getURL() {
        // idk
        return "http://nourl.com";
    }

    @Override
    public List<FederationMember> getMembers() {
        return members;
    }
}
