package se.liu.ida.hefquin.engine.queryplan.executable.impl;

import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;

public interface OpenableIntermediateResultSink extends IntermediateResultElementSink
{
    /**
     * Opens this sink.
     *
     * If this sink is already open, calling this method again has no effect.
     */
    void open();
}