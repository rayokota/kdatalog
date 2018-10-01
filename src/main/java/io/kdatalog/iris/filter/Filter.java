package io.kdatalog.iris.filter;

import io.kdatalog.iris.SimplePDoPRelation;
import io.kdatalog.iris.filter.conditions.Condition;
import org.deri.iris.api.basics.IPredicate;
import org.deri.iris.factory.Factory;
import org.deri.iris.storage.IRelation;

import java.util.HashMap;
import java.util.Map;

public class Filter {
    public static Map<IPredicate, IRelation> filter(Map<IPredicate, IRelation> facts, Condition c) {
        Map<IPredicate, IRelation> filteredFacts = new HashMap<>();
        for (IPredicate factPred : facts.keySet()) {
            String pred = factPred.getPredicateSymbol();
            String[] predWOTime = pred.split("_");
            filteredFacts.put(Factory.BASIC.createPredicate(predWOTime[0], factPred.getArity()), new SimplePDoPRelation());
        }

        for (IPredicate filteredPred : filteredFacts.keySet()) {
            SimplePDoPRelation relAcc = new SimplePDoPRelation();
            for (Map.Entry<IPredicate, IRelation> entry : facts.entrySet()) {
                IPredicate factPred = entry.getKey();
                String[] split = factPred.getPredicateSymbol().split("_");
                if (split[0].equals(filteredPred.getPredicateSymbol())) {
                    if (c.isSatisfied(Long.parseLong(split[1]))) {
                        IRelation factRel = entry.getValue();
                        for (int i = 0; i < factRel.size(); i++) {
                            relAcc.add(factRel.get(i));
                        }
                    }
                }
            }
            filteredFacts.put(filteredPred, relAcc);
        }
        return filteredFacts;
    }
}
