package se.liu.ida.hefquin.engine;

import jakarta.json.JsonObject;
import org.apache.jena.sparql.core.Var;

import java.util.*;
import java.util.stream.Collectors;

public class RandomWalkHolder {
    private List<Var> variablesToGroup;

    private WalksTracker root;

    private class UnionTracker implements WalksTracker, ProbabilityModifier {
        Map<Integer, WalksTracker> childToTracker = new HashMap<>();
        List<String> varNames = new ArrayList<>();

        @Override
        public List<ProbabilityModifier> addWalk(JsonObject json) {

            if(!"union".equals(json.getString("type"))) throw new RuntimeException("json object does not contain a proper 'union' type property");

            Integer childId = json.getInt("child");

            JsonObject sub = json.getJsonObject("sub");

            if(this.varNames.isEmpty()){
                List<String> varNames = json.getJsonArray("vars")
                        .stream()
                        .map(var -> var.toString().substring(2, var.toString().length() - 1))
                        .collect(Collectors.toList());
                this.varNames = varNames;
            }

            if(!childToTracker.keySet().contains(childId)){
                childToTracker.put(childId, build(sub));
            }

            List<ProbabilityModifier> returnList = new ArrayList<>();

            if(variablesToGroup.stream().anyMatch(e -> varNames.contains(e.getVarName()))) returnList.add(this);

            returnList.addAll(childToTracker.get(childId).addWalk(sub));

            return returnList;
        }

        @Override
        public Double getModifier() {
            return Double.valueOf(childToTracker.keySet().size());
        }
    }

    private class JoinTracker implements WalksTracker {
        WalksTracker left;
        WalksTracker right;

        @Override
        public List<ProbabilityModifier> addWalk(JsonObject json) {
            if(!"join".equals(json.getString("type"))) throw new RuntimeException("json object does not contain a proper 'join' type property");

            JsonObject leftJson = json.getJsonObject("left");
            JsonObject rightJson = json.getJsonObject("right");

            left = build(leftJson);
            right = build(rightJson);

            List<ProbabilityModifier> returnList = new ArrayList<>();

            returnList.addAll(left.addWalk(leftJson));
            returnList.addAll(right.addWalk(rightJson));

            return returnList;
        }
    }

    private class ScanTracker implements WalksTracker, ProbabilityModifier {
        List<String> varNames = new ArrayList<>();
        Double scanProbability;

        @Override
        public List<ProbabilityModifier> addWalk(JsonObject json) {
            if(!"scan".equals(json.getString("type"))) throw new RuntimeException("json object does not contain a proper 'scan' type property");

            scanProbability = Double.valueOf(json.getJsonNumber("probability").toString());

            if(Objects.isNull(varNames) || varNames.isEmpty()){
                List<String> varNames = json.getJsonArray("vars")
                        .stream()
                        .map(var -> var.toString().substring(2, var.toString().length() - 1))
                        .collect(Collectors.toList());

                this.varNames = varNames;
            }

            if(variablesToGroup.stream().anyMatch(e -> varNames.contains(e.getVarName()))) return List.of(this);

            return List.of();
        }

        @Override
        public Double getModifier() {
            return Double.valueOf(scanProbability);
        }
    }

    public WalksTracker build(JsonObject jsonObject) {
        String type;
        try {
            type = jsonObject.getString("type");
        } catch (NullPointerException e) {
            throw new RuntimeException("Provided json object does not contain a 'type'");
        } catch (ClassCastException e) {
            throw new RuntimeException("Provided json object does not contain a string value mapped to 'type'");
        }

        switch (type) {
            case "union":
                return new UnionTracker();
            case "join":
                return new JoinTracker();
            case "scan":
                return new ScanTracker();
            default:
                throw new RuntimeException("Random Walk Tracker operator type: ");
        }
    }

    public void initialize(JsonObject rwhJson, List<Var> variablesToGroup) {
        if(Objects.isNull(root)) root = build(rwhJson);
        if(Objects.isNull(this.variablesToGroup)) this.variablesToGroup = variablesToGroup;
    }

    public List<Var> getVariablesToGroup() {
        return variablesToGroup;
    }

    public void setVariablesToGroup(List<Var> variablesToGroup) {
        this.variablesToGroup = variablesToGroup;
    }

    public WalksTracker getRoot() {
        return root;
    }

    public void setRoot(WalksTracker root) {
        this.root = root;
    }

    public List<ProbabilityModifier> addWalk(JsonObject rwhJsonson){
        return root.addWalk(rwhJsonson);
    }
}
