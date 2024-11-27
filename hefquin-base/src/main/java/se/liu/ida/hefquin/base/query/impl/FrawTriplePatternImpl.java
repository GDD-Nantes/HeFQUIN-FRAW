package se.liu.ida.hefquin.base.query.impl;

import org.apache.jena.graph.Triple;
import se.liu.ida.hefquin.base.query.TriplePattern;

public class FrawTriplePatternImpl implements TriplePattern {
    @Override
    public Triple asJenaTriple() {
        return null;
    }

    @Override
    public int numberOfVars() {
        return 0;
    }
}
