package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.federation.*;
import se.liu.ida.hefquin.engine.federation.access.*;
import se.liu.ida.hefquin.engine.federation.access.impl.iface.FederationMemberAgglomerationInterfaceImpl;
import se.liu.ida.hefquin.engine.federation.access.impl.req.AgglomerationRequestImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpRequest;
import se.liu.ida.hefquin.engine.queryplan.physical.NullaryPhysicalOpForLogicalOp;

import java.util.HashSet;
import java.util.Map;

public class PhysicalOpFrawRequest
        extends PhysicalOpRequest<AgglomerationRequest, FederationMemberAgglomeration> implements NullaryPhysicalOpForLogicalOp
{
    public static PhysicalOpFrawRequest instantiate( Map<FederationMember, DataRetrievalRequest> memberToRequest ){
        FederationMemberAgglomerationInterface iface = new FederationMemberAgglomerationInterfaceImpl(memberToRequest.keySet());

        FederationMemberAgglomeration fms = new FederationMemberAgglomeration() {
            @Override public FederationMemberAgglomerationInterface getInterface() {return iface;}
            @Override public VocabularyMapping getVocabularyMapping() {return null;}
        };

        AgglomerationRequest reqs = new AgglomerationRequestImpl(new HashSet<>(memberToRequest.values()));

        LogicalOpRequest<AgglomerationRequest, FederationMemberAgglomeration> lop = new LogicalOpRequest<>(fms, reqs);

        return new PhysicalOpFrawRequest(lop);
    }

    public PhysicalOpFrawRequest( LogicalOpRequest<AgglomerationRequest, FederationMemberAgglomeration> lop ) {
        super(lop);
    }

    @Override
    public boolean equals( final Object o ) {
        return o instanceof PhysicalOpFrawRequest && ((PhysicalOpFrawRequest) o).lop.equals(lop);
    }

    @Override
    public NullaryExecutableOp createExecOp(final boolean collectExceptions,
                                            final ExpectedVariables... inputVars ) {
        final AgglomerationRequest req = lop.getRequest();
        final FederationMember fm = lop.getFederationMember();


        // We have to delegate the endpoint-wise behavior specifications to the exec op itself, otherwise we could only
        // use the random selection request operator with homogeneous groups of federation members
        if ( ! (fm instanceof SPARQLEndpoint && req instanceof SPARQLRequest)
            && ! (fm instanceof TPFServer && req instanceof TriplePatternRequest)
            && ! (fm instanceof BRTPFServer && req instanceof TriplePatternRequest) ) {
            throw new IllegalArgumentException("Unsupported combination of federation member (type: " + fm.getClass().getName() + ") and request type (" + req.getClass().getName() + ")");
        }
        else {
            return null;
//            return new ExecOpFrawRequest(req, fms, collectExceptions);
        }
    }
}
