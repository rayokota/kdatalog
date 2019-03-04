/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kdatalog;

import io.kgraph.AbstractIntegrationTest;
import io.kgraph.GraphAlgorithm;
import io.kgraph.GraphAlgorithmState;
import io.kgraph.KGraph;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

public class DatalogComputationTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(DatalogComputationTest.class);

    private GraphAlgorithm<String, String, String, KTable<String, String>> algorithm;

    private final static String SYMM_RULES = "path(?X, ?Y) :- edge(?X, ?Y).path(?X, ?Z) :- path(?X, ?Y), path(?Y, ?Z).";
    private final static String ASYMM_RULES = "T(?X, ?Y) :- R(?X, ?Y).T(?X, ?Y) :- R(?X, ?Z), T(?Z, ?Y).";

    @Test
    public void testSymmetricComputation() throws Exception {
        String suffix = "dl";
        StreamsBuilder builder = new StreamsBuilder();

        StringBuilder sb = new StringBuilder(SYMM_RULES);
        sb.append("edge('a', 'b').");
        sb.append("edge('c', 'a').");
        sb.append("edge('b', 'c').");
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class,
            StringSerializer.class, new Properties()
        );
        KGraph<String, String, String> graph = KDatalog.createGraph(builder, producerConfig, sb.toString());

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new DatalogComputation());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<String, String>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<String, String> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put("a", SYMM_RULES + "path_3('a', 'a').path_2('a', 'c').path_2('b', 'a')." +
                "path_1('a', 'b').path_1('c', 'a').edge_0('a', 'b').edge_0('c', 'a').");
        expectedResult.put("b", SYMM_RULES + "path_3('b', 'b').path_2('c', 'b').path_2('b', 'a')." +
                "path_1('a', 'b').path_1('b', 'c').edge_0('a', 'b').edge_0('b', 'c').");
        expectedResult.put("c", SYMM_RULES + "path_3('c', 'c').path_2('c', 'b').path_2('a', 'c')." +
                "path_1('c', 'a').path_1('b', 'c').edge_0('c', 'a').edge_0('b', 'c').");
        assertEquals(expectedResult, map);
    }

    @Test
    public void testAsymmetricComputation() throws Exception {
        String suffix = "adl";
        StreamsBuilder builder = new StreamsBuilder();

        StringBuilder sb = new StringBuilder(ASYMM_RULES);
        sb.append("R('1', '2').");
        sb.append("R('2', '1').");
        sb.append("R('1', '4').");
        sb.append("R('2', '3').");
        sb.append("R('3', '4').");
        sb.append("R('1', '4').");
        sb.append("R('4', '5').");
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class,
            StringSerializer.class, new Properties()
        );
        KGraph<String, String, String> graph = KDatalog.createGraph(builder, producerConfig, sb.toString());

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new DatalogComputation());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<String, String>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<String, String> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put("1", ASYMM_RULES + "R_0('1', '2').R_0('2', '1').R_0('1', '4')." +
                "T_2('1', '3').T_2('1', '1').T_2('1', '5').T_1('2', '1').T_1('1', '2').T_1('1', '4').");
        expectedResult.put("2", ASYMM_RULES + "T_3('2', '5').R_0('1', '2').R_0('2', '1')." +
                "R_0('2', '3').T_2('2', '4').T_2('2', '2').T_1('2', '1').T_1('1', '2').T_1('2', '3').");
        expectedResult.put("3", ASYMM_RULES + "R_0('2', '3').R_0('3', '4').T_2('1', '3')." +
                "T_2('3', '5').T_1('2', '3').T_1('3', '4').");
        expectedResult.put("4", ASYMM_RULES + "R_0('1', '4').R_0('3', '4').R_0('4', '5')." +
                "T_2('2', '4').T_1('1', '4').T_1('3', '4').T_1('4', '5').");
        expectedResult.put("5", ASYMM_RULES + "T_3('2', '5').R_0('4', '5').T_2('3', '5')." +
                "T_2('1', '5').T_1('4', '5').");
        assertEquals(expectedResult, map);
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }
}
