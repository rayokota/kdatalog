package io.kdatalog.iris.filter.conditions;

public class TrueCondition implements Condition {

    public boolean isSatisfied(long round) {
        return true;
    }
}
