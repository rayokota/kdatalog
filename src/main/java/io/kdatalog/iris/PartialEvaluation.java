package io.kdatalog.iris;

import org.deri.iris.api.basics.IAtom;
import org.deri.iris.api.basics.ILiteral;
import org.deri.iris.api.basics.IPredicate;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.api.basics.ITuple;
import org.deri.iris.api.terms.ITerm;
import org.deri.iris.api.terms.IVariable;
import org.deri.iris.factory.Factory;
import org.deri.iris.storage.IRelation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartialEvaluation {
    private final Map<IRule, Set<ITerm>> partialRules;

    // partial rules with rule and recipients
    public Map<IRule, Set<ITerm>> getPartialRules() {
        return partialRules;
    }

    private void addPartialRule(IRule rule, Set<ITerm> recipients) {
        partialRules.put(rule, recipients);
    }

    // partial rule for broadcasting
    private final Set<IRule> broadcastRules;

    public Set<IRule> getBroadcastRules() {
        return broadcastRules;
    }

    private void addBroadcastRule(IRule rule) {
        broadcastRules.add(rule);
    }

    /**
     * Partially evaluate rule with given facts.
     */
    public PartialEvaluation(Map<IPredicate, IRelation> facts, IRule rule) {
        partialRules = new HashMap<>();
        broadcastRules = new HashSet<>();
        for (ILiteral literal : rule.getBody()) {
            IRelation values = facts.get(literal.getAtom().getPredicate());
            if (values != null) {
                for (int i = 0; i < values.size(); i++) {
                    IRule interRule = changeRule(values.get(i), literal, rule);
                    if (interRule != null) {
                        interRule = checkRule(facts, interRule);
                        if (interRule != null) {
                            if (!rule.equals(interRule)) {
                                Set<ITerm> recipients = getRecipientsFromRule(interRule);
                                if (recipients.isEmpty()) {
                                    addBroadcastRule(interRule);
                                } else {
                                    addPartialRule(interRule, recipients);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static Set<ITerm> getRecipientsFromRule(IRule rule) {
        Set<ITerm> recipients = new HashSet<>();
        if (rule.getBody().isEmpty()) {
            recipients.addAll(checkSegmentForRecipients(rule.getHead()));
        } else {
            recipients.addAll(checkSegmentForRecipients(rule.getBody()));
        }
        return recipients;
    }

    public static Set<ITerm> checkSegmentForRecipients(List<ILiteral> segment) {
        Set<ITerm> recipients = new HashSet<>();
        for (ILiteral literal : segment) {
            for (ITerm term : literal.getAtom().getTuple()) {
                if (!(term instanceof IVariable)) {
                    recipients.add(term);
                }
            }
        }
        return recipients;
    }

    private static IRule checkRule(Map<IPredicate, IRelation> facts, IRule rule) {
        List<ILiteral> changedBody = new LinkedList<>();
        // check rules for facts and remove parts that are true
        // (aka represented in the facts)
        for (int i = 0; i < rule.getBody().size(); i++) {
            IAtom atomToCheck = rule.getBody().get(i).getAtom();
            boolean builtIn = isBuiltIn(atomToCheck.getPredicate());
            if (builtIn) {
                if (checkBuiltIn(changedBody, atomToCheck)) {
                    return null;
                }
            } else {
                IRelation edb = facts.get(atomToCheck.getPredicate());
                if (edb != null && edb.size() != 0) {
                    boolean anyEDBTrue = false;
                    for (int j = 0; j < edb.size(); j++) {
                        boolean edbTrue = true;
                        for (int k = 0; k < atomToCheck.getTuple().size(); k++) {
                            if (atomToCheck.getTuple().get(k).getValue().toString().equals(
                                edb.get(j).get(k).getValue().toString())) {
                                // no-op
                            } else {
                                edbTrue = false;
                                break;
                            }
                        }
                        if (edbTrue) {
                            anyEDBTrue = true;
                            break;
                        }
                    }
                    if (!anyEDBTrue) {
                        changedBody.add(Factory.BASIC.createLiteral(true, atomToCheck));
                    }
                } else {
                    if (atomToCheck.getTuple().getAllVariables().size() == 0) {
                        // TODO: p(d) :- t(a). still possible
                        return null;
                    } else {
                        changedBody.add(Factory.BASIC.createLiteral(true, atomToCheck));
                    }
                }
            }
        }
        return Factory.BASIC.createRule(rule.getHead(), changedBody);
    }

    private static IRule changeRule(ITuple fact, ILiteral literal, IRule rule) {
        Map<IVariable, ITerm> varMap = new HashMap<>();

        // add every variable to the map and save the string that matches given the facts
        for (int i = 0; i < literal.getAtom().getTuple().size(); i++) {
            ITerm term = literal.getAtom().getTuple().get(i);
            if (term instanceof IVariable) {
                varMap.put((IVariable) term, fact.get(i));
            } else {
                if (!term.equals(fact.get(i))) {
                    return null;
                }
            }
        }

        List<ILiteral> changedHead = changeSegment(varMap, rule.getHead());
        List<ILiteral> changedBody = changeSegment(varMap, rule.getBody());
        return Factory.BASIC.createRule(changedHead, changedBody);
    }

    /*
     * Replaces the variables in the literals with the appropriate constants from the varMap.
     */
    private static List<ILiteral> changeSegment(Map<IVariable, ITerm> varMap,
                                                List<ILiteral> literals) {
        List<ILiteral> newLiterals = new LinkedList<>();
        for (ILiteral literal : literals) {
            ITuple tuple = literal.getAtom().getTuple();
            List<ITerm> newTerms = new LinkedList<>();
            for (ITerm aTuple : tuple) {
                ITerm newTerm = varMap.get(aTuple);
                if (newTerm == null) {
                    newTerms.add(aTuple);
                } else {
                    newTerms.add(newTerm);
                }
            }
            ILiteral newLiteral = Factory.BASIC.createLiteral(
                true, literal.getAtom().getPredicate(), Factory.BASIC.createTuple(newTerms));
            newLiterals.add(newLiteral);
        }
        return newLiterals;
    }

    /*
     * Checks if atomToCheck is a verifiable with a built-in function.
     */
    private static boolean checkBuiltIn(List<ILiteral> changedBody,
                                        IAtom atomToCheck) {
        // TODO: check for other built-in functions besides NOT_EQUAL
        if (atomToCheck.getPredicate().getPredicateSymbol().equals("NOT_EQUAL")) {
            if (atomToCheck.getTuple().get(0).getValue().toString().equals(
                atomToCheck.getTuple().get(1).getValue().toString())) {
                return true;
            } else {
                if (atomToCheck.getTuple().getAllVariables().size() != 0) {
                    changedBody.add(Factory.BASIC.createLiteral(true, atomToCheck));
                }
            }
        }
        return false;
    }

    private static boolean isBuiltIn(IPredicate pred) {
        return pred.getPredicateSymbol().equals("NOT_EQUAL");
    }
}