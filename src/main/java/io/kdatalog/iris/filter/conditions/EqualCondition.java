package io.kdatalog.iris.filter.conditions;

public class EqualCondition implements Condition {
    private final long value;

    public EqualCondition(long v) {
        value = v;
    }

    public boolean isSatisfied(long round) {
        return round == value;
    }
}
