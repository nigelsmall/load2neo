/*
 * Copyright 2013, Nigel Small
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nigelsmall.load2neo;

import org.neo4j.cypher.UniquePathNotUniqueException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.IteratorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NeoLoader {

    final private Logger logger = LoggerFactory.getLogger(NeoLoader.class);
    final private GraphDatabaseService database;
    final private ExecutionEngine engine;

    public NeoLoader(GraphDatabaseService database) {
        this.database = database;
        this.engine = new ExecutionEngine(this.database);
    }

    /**
     * Load a subgraph into the database.
     *
     * @param subgraph the subgraph to load
     * @return a Map of named Nodes that have been loaded
     */
    public Map<String, Node> load(Subgraph subgraph) {
        // gather entities and stats
        Map<String, AbstractNode> abstractNodes = subgraph.getNodes();
        List<AbstractRelationship> abstractRelationships = subgraph.getRelationships();
        int order = subgraph.order();
        int size = subgraph.size();
        HashMap<String, Node> nodes = new HashMap<>(order);
        HashMap<String, Node> namedNodes = new HashMap<>(order);
        // start load
        logger.info(String.format("Loading subgraph with %d nodes and %d relationships...",
                    order, size));
        long t0 = System.currentTimeMillis();
        // load nodes
        for (AbstractNode abstractNode : abstractNodes.values()) {
            Node node = this.loadNode(abstractNode);
            nodes.put(abstractNode.getName(), node);
            if (abstractNode.isNamed()) {
                namedNodes.put(abstractNode.getName(), node);
            }
        }
        // load relationships
        for (AbstractRelationship abstractRelationship : abstractRelationships) {
            this.loadRelationship(abstractRelationship, nodes);
        }
        // finish load
        long t1 = System.currentTimeMillis() - t0;
        logger.info(String.format("Loaded subgraph with %d nodes and %d relationships in %dms", order, size, t1));
        return namedNodes;
    }

    /**
     * Create or merge a node. If this is a unique node, a merge will occur,
     * otherwise a new node will be created.
     *
     * @param abstractNode an abstract node specification
     * @return the concrete Node object that is either fetched or created
     */
    public Node loadNode(AbstractNode abstractNode) {
        Node node = null;
        if (abstractNode.isUnique()) {
            // determine the label, key and value to look up
            Label label = DynamicLabel.label(abstractNode.getUniqueLabel());
            String uniqueKey = abstractNode.getUniqueKey();
            Object uniqueValue = null;
            if (abstractNode.getProperties().containsKey(uniqueKey)) {
                uniqueValue = abstractNode.getProperties().get(uniqueKey);
            }
            // find the "first" node with the given label, key and value
            for (Node foundNode : database.findNodesByLabelAndProperty(label, uniqueKey, uniqueValue)) {
                node = foundNode;
                break;
            }
        }
        // if not unique, or cannot find, create anew
        if (node == null) {
            node = database.createNode();
        }
        this.addLabels(node, abstractNode.getLabels());
        this.addProperties(node, abstractNode.getProperties());
        return node;
    }

    public void loadRelationship(AbstractRelationship abstractRelationship, HashMap<String, Node> nodes) {
        Node startNode = nodes.get(abstractRelationship.getStartNode().getName());
        Node endNode = nodes.get(abstractRelationship.getEndNode().getName());
        if (abstractRelationship.isUnique()) {
            String type = abstractRelationship.getType().replace("`", "``");
            String query;
            HashMap<String, Object> params = new HashMap<>(3);
            params.put("a", startNode.getId());
            params.put("b", endNode.getId());
            if (abstractRelationship.getProperties() == null) {
                query = "START a=node({a}), b=node({b}) CREATE UNIQUE (a)-[ab:`" + type + "`]->(b) RETURN ab";
            } else {
                query = "START a=node({a}), b=node({b}) CREATE UNIQUE (a)-[ab:`" + type + "` {p}]->(b) RETURN ab";
                params.put("p", abstractRelationship.getProperties());
            }
            this.engine.execute(query, params);
        } else {
            DynamicRelationshipType type = DynamicRelationshipType.withName(abstractRelationship.getType());
            Relationship rel = startNode.createRelationshipTo(endNode, type);
            this.addProperties(rel, abstractRelationship.getProperties());
        }
    }

    /**
     * Add a set of labels to a node.
     *
     * @param node the destination Node to which to add the labels
     * @param labels a set of strings containing label names
     */
    public void addLabels(Node node, Set<String> labels) {
        if (labels == null)
            return;
        for (String label : labels) {
            node.addLabel(DynamicLabel.label(label));
        }
    }

    /**
     * Add a map of properties to a node or relationship.
     *
     * @param entity the destination Node or Relationship to which to add the properties
     * @param properties a Map of key-value property pairs
     */
    public void addProperties(PropertyContainer entity, Map<String, Object> properties) {
        if (properties == null)
            return;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() != null) {
                entity.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

}
