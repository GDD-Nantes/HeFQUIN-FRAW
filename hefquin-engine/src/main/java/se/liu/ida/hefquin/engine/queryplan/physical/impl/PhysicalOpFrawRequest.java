package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.federation.*;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalRequest;
import se.liu.ida.hefquin.engine.federation.access.FederationMemberAgglomerationInterface;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.TriplePatternRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.iface.FederationMemberAgglomerationInterfaceImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawRequest;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpRequest;
import se.liu.ida.hefquin.engine.queryplan.physical.NullaryPhysicalOpForLogicalOp;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class PhysicalOpFrawRequest
        extends PhysicalOpRequest<DataRetrievalRequest, FederationMemberAgglomeration> implements NullaryPhysicalOpForLogicalOp
{
    public static PhysicalOpFrawRequest instantiate( Map<FederationMember, DataRetrievalRequest> memberToRequest ){

        // Inefficient, because might be called multiple times over the course of the creation of the physical plan
        List<FederationMember> flattenedFederatonMembers = memberToRequest.keySet().stream().flatMap(member -> {
            if(member instanceof FederationMemberAgglomeration){
                return ((FederationMemberAgglomeration) member).getInterface().getMembers().stream();
            }else {
                return List.of(member).stream();
            }
        }).toList();

        FederationMemberAgglomerationInterface iface = new FederationMemberAgglomerationInterfaceImpl(flattenedFederatonMembers);

        FederationMemberAgglomeration fms = new FederationMemberAgglomeration() {
            @Override public FederationMemberAgglomerationInterface getInterface() {return iface;}
            @Override public VocabularyMapping getVocabularyMapping() {return null;}
        };

        Iterator<DataRetrievalRequest> requests = memberToRequest.values().stream().distinct().iterator();
        DataRetrievalRequest request = requests.next();

        try {
            requests.next();
            throw new UnsupportedOperationException("There can only be one request!");
        } catch (NoSuchElementException e){
            // Nothing to do, this is normal behaviour
        }

        LogicalOpRequest<DataRetrievalRequest, FederationMemberAgglomeration> lop = new LogicalOpRequest<>(fms, request);

        return new PhysicalOpFrawRequest(lop);
    }

    public PhysicalOpFrawRequest( LogicalOpRequest<DataRetrievalRequest, FederationMemberAgglomeration> lop ) {
        super(lop);
    }

    @Override
    public boolean equals( final Object o ) {
        return o instanceof PhysicalOpFrawRequest && ((PhysicalOpFrawRequest) o).lop.equals(lop);
    }

    @Override
    public NullaryExecutableOp createExecOp(final boolean collectExceptions,
                                            final ExpectedVariables... inputVars ) {
        final DataRetrievalRequest req = lop.getRequest();
        final FederationMemberAgglomeration fm =  lop.getFederationMember();


        // We have to delegate the endpoint-wise behavior specifications to the exec op itself, otherwise we could only
        // use the random selection request operator with homogeneous groups of federation members
        if ( ! (fm instanceof SPARQLEndpoint && req instanceof SPARQLRequest)
                && ! (fm instanceof TPFServer && req instanceof TriplePatternRequest)
                && ! (fm instanceof BRTPFServer && req instanceof TriplePatternRequest)
                && ! (fm instanceof FederationMemberAgglomeration && req instanceof SPARQLRequest)) {
            throw new IllegalArgumentException("Unsupported combination of federation member (type: " + fm.getClass().getName() + ") and request type (" + req.getClass().getName() + ")");
        }
        else {
            return new ExecOpFrawRequest(req, fm, collectExceptions);
        }
    }
}
