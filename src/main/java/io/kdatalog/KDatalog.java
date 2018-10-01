package io.kdatalog;

import io.kdatalog.iris.RuleFactQuerySplitter;
import io.kgraph.Edge;
import io.kgraph.GraphSerialized;
import io.kgraph.KGraph;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.deri.iris.api.basics.IPredicate;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.api.basics.ITuple;
import org.deri.iris.api.terms.ITerm;
import org.deri.iris.storage.IRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KDatalog {
    private static final Logger log = LoggerFactory.getLogger(KDatalog.class);

    public static KGraph<String, String, String> createGraph(StreamsBuilder builder,
                                                             Properties producerConfig,
                                                             String program) {
        return createGraph(builder, producerConfig, new StringReader(program));
    }

    public static KGraph<String, String, String> createGraph(StreamsBuilder builder,
                                                             Properties producerConfig,
                                                             Reader program) {
        RuleFactQuerySplitter splitter = new RuleFactQuerySplitter(program);
        List<IRule> rules = splitter.getRules();

        Map<String, Map<IPredicate, List<ITuple>>> tuplesByTerms = new HashMap<>();
        Map<IPredicate, IRelation> facts = splitter.getFacts();
        for (Map.Entry<IPredicate, IRelation> entry : facts.entrySet()) {
            IPredicate pred = entry.getKey();
            IRelation rel = entry.getValue();
            for (int i = 0; i < rel.size(); i++) {
                ITuple tuple = rel.get(i);
                for (ITerm term : tuple) {
                    Map<IPredicate, List<ITuple>> tuplesByPred =
                        tuplesByTerms.computeIfAbsent(term.getValue().toString(), k -> new HashMap<>());
                    List<ITuple> tuples = tuplesByPred.computeIfAbsent(pred, k -> new ArrayList<>());
                    tuples.add(tuple);
                }
            }
        }

        List<KeyValue<Edge<String>, String>> edges = createEdges(tuplesByTerms.keySet());
        KTable<Edge<String>, String> edgesTable =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.String(), edges);

        KGraph<String, String, String> graph = KGraph.fromEdges(edgesTable, new InitVertices(rules, tuplesByTerms),
            GraphSerialized.with(Serdes.String(), Serdes.String(), Serdes.String()));
        return graph;
    }

    private static List<KeyValue<Edge<String>, String>> createEdges(Set<String> vertices) {
        List<KeyValue<Edge<String>, String>> edges = new ArrayList<>();
        for (String vertex1 : vertices) {
            for (String vertex2 : vertices) {
                if (!vertex1.equals(vertex2)) {
                    edges.add(new KeyValue<>(new Edge<>(vertex1, vertex2), ""));
                }
            }
        }
        return edges;
    }

    private static class InitVertices implements ValueMapper<String, String> {
        private final List<IRule> rules;
        private final Map<String, Map<IPredicate, List<ITuple>>> tuplesByTerm;

        public InitVertices(List<IRule> rules, Map<String, Map<IPredicate, List<ITuple>>> tuplesByTerm) {
            this.rules = rules;
            this.tuplesByTerm = tuplesByTerm;
        }

        @Override
        public String apply(String id) {
            StringBuilder sb = new StringBuilder();
            for (IRule rule : rules) {
                sb.append(rule.toString());
            }
            Map<IPredicate, List<ITuple>> tuplesByPred = tuplesByTerm.get(id);
            for (Map.Entry<IPredicate, List<ITuple>> entry : tuplesByPred.entrySet()) {
                IPredicate pred = entry.getKey();
                for (ITuple tuple : entry.getValue()) {
                    sb.append(pred.toString());
                    sb.append(tuple.toString());
                    sb.append(".");
                }
            }
            return sb.toString();
        }
    }
}
