package io.kdatalog;

import io.kdatalog.iris.PartialEvaluation;
import io.kdatalog.iris.RuleFactQuerySplitter;
import io.kdatalog.iris.SimplePDoPRelation;
import io.kdatalog.iris.filter.Filter;
import io.kdatalog.iris.filter.conditions.EqualCondition;
import io.kdatalog.iris.filter.conditions.LessEqualCondition;
import io.kdatalog.iris.filter.conditions.TrueCondition;
import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.pregel.aggregators.BooleanAndAggregator;
import io.kgraph.pregel.aggregators.BooleanOrAggregator;
import io.kgraph.pregel.aggregators.LongMaxAggregator;
import org.deri.iris.api.basics.IAtom;
import org.deri.iris.api.basics.IPredicate;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.api.basics.ITuple;
import org.deri.iris.api.terms.ITerm;
import org.deri.iris.factory.Factory;
import org.deri.iris.storage.IRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DatalogComputation extends PregelGraphAlgorithm<String, String, String, String> {
    private static final Logger log = LoggerFactory.getLogger(DatalogComputation.class);

    private static final String ROUND_AGGREGATOR = "RoundAggregator";
    private static final String VOTE_AGGREGATOR = "VoteAggregator";
    private static final String LAST_TIME_AGGREGATOR = "LastTimeAggregator";
    private static final String DUMMY_MSG = "";

    public DatalogComputation(String hostAndPort,
                              String applicationId,
                              String bootstrapServers,
                              String zookeeperConnect,
                              String verticesTopic,
                              String edgesGroupedBySourceTopic,
                              GraphSerialized<String, String, String> serialized,
                              String solutionSetTopic,
                              String solutionSetStore,
                              String workSetTopic,
                              int numPartitions,
                              short replicationFactor
    ) {
        super(hostAndPort, applicationId, bootstrapServers, zookeeperConnect, verticesTopic, edgesGroupedBySourceTopic, serialized,
            solutionSetTopic, solutionSetStore, workSetTopic, numPartitions, replicationFactor, Optional.empty());

        registerAggregator(ROUND_AGGREGATOR, LongMaxAggregator.class);
        registerAggregator(VOTE_AGGREGATOR, BooleanAndAggregator.class);
        registerAggregator(LAST_TIME_AGGREGATOR, BooleanOrAggregator.class);
    }

    @Override
    protected ComputeFunction<String, String, String, String> computeFunction() {
        return new DatalogComputeFunction();
    }

    public static class DatalogComputeFunction implements ComputeFunction<String, String, String, String> {

        @Override
        public void preSuperstep(int superstep, Aggregators agg) {
            Long round = agg.getAggregatedValue(ROUND_AGGREGATOR);
            if (round == null) round = 0L;
            Boolean voteForNewRound = agg.getAggregatedValue(VOTE_AGGREGATOR);
            if (voteForNewRound == null) voteForNewRound = true;

            if (voteForNewRound) round++;

            agg.aggregate(ROUND_AGGREGATOR, round);
            agg.aggregate(LAST_TIME_AGGREGATOR, voteForNewRound);
        }

        @Override
        public void compute(
            int superstep,
            VertexWithValue<String, String> vertex,
            Iterable<String> messages,
            Iterable<EdgeWithValue<String, String>> edges,
            Callback<String, String, String> cb) {

            Long round = cb.getAggregatedValue(ROUND_AGGREGATOR);
            if (round == null) round = 0L;
            Boolean voteForNewRound = cb.getAggregatedValue(VOTE_AGGREGATOR);
            if (voteForNewRound == null) voteForNewRound = true;
            Boolean lastTimeNewRound = cb.getAggregatedValue(LAST_TIME_AGGREGATOR);
            if (lastTimeNewRound == null) lastTimeNewRound = false;

            if (voteForNewRound) round++;

            log.debug(">>> Superstep {} (round {}), vertex {}", superstep, round, vertex.id());
            log.debug(">>> Received messages {}", messages);

            if (voteForNewRound && lastTimeNewRound) {
                voteToHalt(cb, vertex);
            } else {
                boolean voteForNextRound;

                RuleFactQuerySplitter splitter = new RuleFactQuerySplitter(vertex.value());
                List<IRule> rules = splitter.getRules();
                Map<IPredicate, IRelation> facts = splitter.getFacts();

                if (superstep == 0) {
                    // Add timestamps to first facts.
                    Map<IPredicate, IRelation> factsWtime = new HashMap<>();
                    for (Map.Entry<IPredicate, IRelation> entry : facts.entrySet()) {
                        IPredicate pred = entry.getKey();
                        factsWtime.put(Factory.BASIC.createPredicate(pred.getPredicateSymbol() + "_0",
                            pred.getArity()), entry.getValue());
                    }
                    facts = factsWtime;
                    /*
                     * All facts look like this now: a_0('a', 'b').
                     * These are set to the vertex value at the end of compute.
                     */
                }

                StringBuilder ruleBuilder = new StringBuilder();
                for (String msg : messages) {
                    if (!msg.equals(DUMMY_MSG)) {
                        ruleBuilder.append(msg);
                    }
                }

                RuleFactQuerySplitter msgSplitter = new RuleFactQuerySplitter(ruleBuilder.toString());

                List<IRule> msgRules = msgSplitter.getRules();
                List<IRule> partialRules = new LinkedList<>();

                Map<IPredicate, IRelation> msgedFacts = new HashMap<>();
                Map<IPredicate, IRelation> truncatedFacts = Filter.filter(facts, new TrueCondition());

                // Check msgRules for real facts and partial rules.
                for (IRule msgRule : msgRules) {
                    if (msgRule.getBody().size() == 0) { // add a fact
                        IAtom headAtom = msgRule.getHead().get(0).getAtom();
                        ITuple headTuple = headAtom.getTuple();
                        IPredicate factPred = headAtom.getPredicate();
                        IRelation tuplesOfPredicate = truncatedFacts.get(factPred);
                        if (tuplesOfPredicate == null || !(tuplesOfPredicate.contains(headTuple))) {
                            // If fact is not there then add; otherwise do nothing.
                            String factPredSymbol = factPred.getPredicateSymbol() + "_" + round;
                            IPredicate newFactPred = Factory.BASIC.createPredicate(factPredSymbol, headTuple.size());
                            IRelation factRel;
                            if (msgedFacts.containsKey(newFactPred)) {
                                factRel = msgedFacts.get(newFactPred);
                            } else {
                                factRel = new SimplePDoPRelation();
                            }
                            factRel.add(headTuple);
                            msgedFacts.put(newFactPred, factRel);
                        }
                    } else {
                        // add rule as partialRule
                        partialRules.add(msgRule);
                    }
                }
                facts.putAll(msgedFacts);

                /*
                 * Vertex value is set with these rules and facts.
                 * Following only messages are generated with these rules, but no local rules or facts are changed.
                 */

                if (superstep == 0) {
                    // First evaluation of rules.
                    voteForNextRound = evaluateThis(truncatedFacts, rules, edges, cb);
                } else {
                    // If this is a new round then use deltaFacts and evaluate with all rules.
                    if (voteForNewRound) {
                        Map<IPredicate, IRelation> deltaFacts = Filter.filter(facts, new EqualCondition(round - 1));
                        // TODO: Remove facts with vertex ID not appearing as first argument from deltaFacts.
                        voteForNextRound = evaluateThis(deltaFacts, rules, edges, cb);
                    } else {
                        // If this is not a new round then use all facts from the previous rounds
                        // and evaluate only with partial rules.
                        Map<IPredicate, IRelation> prevFacts = Filter.filter(facts, new LessEqualCondition(round - 1));
                        voteForNextRound = evaluateThis(prevFacts, partialRules, edges, cb);
                    }
                }
                cb.aggregate(VOTE_AGGREGATOR, voteForNextRound);

                // Update vertex value.
                StringBuilder builder = new StringBuilder();
                for (IRule rule : rules) {
                    builder.append(rule);
                }
                for (Map.Entry<IPredicate, IRelation> entry : facts.entrySet()) {
                    IPredicate pred = entry.getKey();
                    IRelation rel = entry.getValue();
                    for (int i = 0; i < rel.size(); i++) {
                        builder.append(pred);
                        builder.append(rel.get(i));
                        builder.append(".");
                    }
                }
                setVertexValue(cb, builder.toString());
            }
        }

        public boolean evaluateThis(Map<IPredicate, IRelation> facts,
                                    List<IRule> rules,
                                    Iterable<EdgeWithValue<String, String>> edges,
                                    Callback<String, String, String> cb) {
            boolean voteForNextRound = false;

            Map<IRule, Set<ITerm>> cleanedMessages = new HashMap<>();
            Set<IRule> broadcasts = new HashSet<>();
            for (IRule rule : rules) {
                PartialEvaluation eval = new PartialEvaluation(facts, rule);
                Map<IRule, Set<ITerm>> partialRules = eval.getPartialRules();
                for (Map.Entry<IRule, Set<ITerm>> entry : partialRules.entrySet()) {
                    IRule msg = entry.getKey();
                    if (!cleanedMessages.containsKey(msg)) {
                        cleanedMessages.put(msg, new HashSet<>());
                    }
                    cleanedMessages.get(msg).addAll(entry.getValue());
                }
                broadcasts.addAll(eval.getBroadcastRules());
            }
            for (Map.Entry<IRule, Set<ITerm>> entry : cleanedMessages.entrySet()) {
                IRule msg = entry.getKey();
                for (ITerm recipient : entry.getValue()) {
                    sendMessage(cb, recipient.getValue().toString(), msg.toString());
                }
            }

            for (IRule broadcast : broadcasts) {
                for (EdgeWithValue<String, String> edge : edges) {
                    sendMessage(cb, edge.target(), broadcast.toString());
                }
            }
            if (cleanedMessages.isEmpty() && broadcasts.isEmpty()) {
                voteForNextRound = true; // no messages were sent
            }
            return voteForNextRound;
        }

        private void sendMessage(Callback<String, String, String> cb, String target, String message) {
            cb.sendMessageTo(target, message);
        }

        private void setVertexValue(Callback<String, String, String> cb, String value) {
            cb.setNewVertexValue(value);
        }

        private void voteToHalt(Callback<String, String, String> cb, VertexWithValue<String, String> vertex) {
            log.debug(">>> Vote to halt: vertex {}", vertex.id());
            cb.voteToHalt();
        }
    }
}
