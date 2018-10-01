package io.kdatalog.iris.filter.conditions;

public class LessEqualCondition implements Condition {
    private final long value;

    public LessEqualCondition(long v) {
        value = v;
    }

    public boolean isSatisfied(long round) {
        return round <= value;
    }
}
