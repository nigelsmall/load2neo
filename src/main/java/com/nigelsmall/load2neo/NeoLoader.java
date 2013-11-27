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

import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NeoLoader {

    final private Logger logger = LoggerFactory.getLogger(NeoLoader.class);
    final private GraphDatabaseService database;

    public NeoLoader(GraphDatabaseService database) {
        this.database = database;
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
            Node node = this.createOrUpdateNode(abstractNode);

            nodes.put(abstractNode.getName(), node);
            if (abstractNode.isNamed()) {
                namedNodes.put(abstractNode.getName(), node);
            }
        }
        // load relationships
        for (AbstractRelationship abstractRelationship : abstractRelationships) {
            Node startNode = nodes.get(abstractRelationship.getStartNode().getName());
            Node endNode = nodes.get(abstractRelationship.getEndNode().getName());
            if(startNode != null && endNode != null) {
                DynamicRelationshipType type = DynamicRelationshipType.withName(abstractRelationship.getType());
                Relationship rel = startNode.createRelationshipTo(endNode, type);
                this.addProperties(rel, abstractRelationship.getProperties());
            }
        }
        // finish load
        long t1 = System.currentTimeMillis() - t0;
        logger.info(String.format("Loaded subgraph with %d nodes and %d relationships in %dms", order, size, t1));
        return namedNodes;
    }

    /**
     * Create a new node or update an existing one. An update will occur only
     * if this is a hooked node specification and a match can be found.
     *
     * @param abstractNode an abstract node specification
     * @return the concrete Node object that is either fetched or created
     */
    public Node createOrUpdateNode(AbstractNode abstractNode) {
        Node foundNode = null;
        String hookLabel = abstractNode.getHookLabel();
        // is this a hooked node?
        if (hookLabel != null) {

            // determine the label, key and value to look up
            Label label = DynamicLabel.label(hookLabel);
            String[] hookKeys = abstractNode.getHookKeys();
            Object hookValue = null;

            // in order to support multiple keys we need to match each key separately
            // and the final result is the intersection of all results
            HashMap<Long, Node> matchingNodesById = new HashMap<>();

            for(int i = 0; i < hookKeys.length; i++) {

                String hookKey = hookKeys[i];

                if (abstractNode.getProperties().containsKey(hookKey)) {
                    hookValue = abstractNode.getProperties().get(hookKey);
                }

                Iterable<Node> nodesMatchingCurrentKey = database.findNodesByLabelAndProperty(label, hookKey, hookValue);

                if(i == 0) {
                    // first key, so add all matching nodes to result
                    for(Node node : nodesMatchingCurrentKey)
                        matchingNodesById.put(node.getId(), node);
                }
                else {
                    // not first key, so remove items that are not present in this result
                    // (effectively performing a continual intersection of the results)
                    Iterator<Map.Entry<Long, Node>> it = matchingNodesById.entrySet().iterator();
                    HashSet<Long> nodeIdsMatchingCurrentKey = new HashSet<>();

                    for(Node node : nodesMatchingCurrentKey)
                        nodeIdsMatchingCurrentKey.add(node.getId());

                    while (it.hasNext()) {

                        Long nodeId = it.next().getKey();

                        if(!nodeIdsMatchingCurrentKey.contains(nodeId))
                            it.remove();
                    }
                }
            }

            if(matchingNodesById.size() > 0)
                foundNode = matchingNodesById.values().iterator().next();
        }

        // if not hooked, or cannot find, create anew unless the hook is optional
        if (foundNode == null) {
            if(abstractNode.isHookOptional())
                return null;
            foundNode = database.createNode();
        }

        this.addLabels(foundNode, abstractNode.getLabels());
        this.addProperties(foundNode, abstractNode.getProperties());
        return foundNode;
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
