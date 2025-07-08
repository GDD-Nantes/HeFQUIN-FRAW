package se.liu.ida.hefquin.engine.federation.access.impl.req;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.engine.federation.access.AgglomerationRequest;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalRequest;

import java.util.Set;

public class AgglomerationRequestImpl implements AgglomerationRequest {

    Set<DataRetrievalRequest> requests;

    public AgglomerationRequestImpl(Set<DataRetrievalRequest> requests){
        this.requests = requests;
    }

    @Override
    public ExpectedVariables getExpectedVariables() {
        // TODO:
        return null;
    }
}
