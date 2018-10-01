package io.kdatalog.iris.filter.conditions;

public interface Condition {
    boolean isSatisfied(long round);
}
