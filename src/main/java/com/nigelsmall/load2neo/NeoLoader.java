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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NeoLoader {

    final private Logger logger = LoggerFactory.getLogger(NeoLoader.class);
    final private GraphDatabaseService database;
    final private HashMap<String, Label> labelCache;
    final private HashMap<String, RelationshipType> typeCache;

    public NeoLoader(GraphDatabaseService database) {
        this.database = database;
        this.labelCache = new HashMap<String, Label>();
        this.typeCache = new HashMap<String, RelationshipType>();
    }

    private Label getLabel(String name) {
        if (!this.labelCache.containsKey(name)) {
            this.labelCache.put(name, DynamicLabel.label(name));
        }
        return this.labelCache.get(name);
    }

    private RelationshipType getType(String name) {
        if (!this.typeCache.containsKey(name)) {
            this.typeCache.put(name, DynamicRelationshipType.withName(name));
        }
        return this.typeCache.get(name);
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
            Label label = this.getLabel(abstractNode.getUniqueLabel());
            String uniqueKey = abstractNode.getUniqueKey();
            Object uniqueValue = abstractNode.getUniqueValue();
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
        this.setLabels(node, abstractNode.getLabels());
        this.setProperties(node, abstractNode.getProperties());
        return node;
    }

    public void loadRelationship(AbstractRelationship abstractRelationship, HashMap<String, Node> nodes) {
        Node startNode = nodes.get(abstractRelationship.getStartNode().getName());
        Node endNode = nodes.get(abstractRelationship.getEndNode().getName());
        RelationshipType type = this.getType(abstractRelationship.getType());
        Map<String, Object> properties = abstractRelationship.getProperties();
        Relationship rel;
        if (abstractRelationship.isUnique()) {
            String uniqueKey = abstractRelationship.getUniqueKey();
            if (uniqueKey == null) {
                rel = mergeRelationship(startNode, endNode, type);
            } else {
                rel = mergeRelationship(startNode, endNode, type, uniqueKey, abstractRelationship.getUniqueValue());
            }
        } else {
            rel = startNode.createRelationshipTo(endNode, type);
        }
        this.setProperties(rel, properties);
    }

    public Relationship mergeRelationship(Node startNode, Node endNode, RelationshipType type) {
        Relationship existingRelationship = null;
        for (Relationship rel : startNode.getRelationships(type, Direction.OUTGOING)) {
            if (rel.getEndNode().equals(endNode)) {
                existingRelationship = rel;
                break;
            }
        }
        if (existingRelationship == null) {
            return startNode.createRelationshipTo(endNode, type);
        } else {
            return existingRelationship;
        }
    }

    public Relationship mergeRelationship(Node startNode, Node endNode, RelationshipType type, String key, Object value) {
        Relationship existingRelationship = null;
        for (Relationship rel : startNode.getRelationships(type, Direction.OUTGOING)) {
            if (rel.getEndNode().equals(endNode) && rel.hasProperty(key) && rel.getProperty(key).equals(value)) {
                existingRelationship = rel;
                break;
            }
        }
        if (existingRelationship == null) {
            return startNode.createRelationshipTo(endNode, type);
        } else {
            return existingRelationship;
        }
    }

    /**
     * Add a set of labels to a node.
     *
     * @param node the destination Node to which to add the labels
     * @param labels a set of strings containing label names
     */
    public void setLabels(Node node, Set<String> labels) {
        if (labels == null)
            return;
        for (String label : labels) {
            node.addLabel(this.getLabel(label));
        }
    }

    /**
     * Add a map of properties to a node or relationship.
     *
     * @param entity the destination Node or Relationship to which to add the properties
     * @param properties a Map of key-value property pairs
     */
    public void setProperties(PropertyContainer entity, Map<String, Object> properties) {
        if (properties == null)
            return;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() != null) {
                entity.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

}
