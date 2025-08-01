package se.liu.ida.hefquin.engine;

import jakarta.json.JsonObject;

import java.util.List;

public interface WalksTracker {
    List<ProbabilityModifier> addWalk(JsonObject json);
}
