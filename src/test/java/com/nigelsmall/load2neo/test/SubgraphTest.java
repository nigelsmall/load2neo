package com.nigelsmall.load2neo.test;

import com.nigelsmall.load2neo.AbstractNode;
import com.nigelsmall.load2neo.AbstractRelationship;
import com.nigelsmall.load2neo.Subgraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SubgraphTest {

    final private Logger logger = LoggerFactory.getLogger(SubgraphTest.class);

    final private Subgraph subgraph;
    final private Map<String, AbstractNode> nodes;
    final private Map<String, List<AbstractRelationship>> relationships;

    public SubgraphTest(Subgraph subgraph) {
        this.subgraph = subgraph;
        this.nodes = subgraph.getNodes();
        this.relationships = new HashMap<>();
        for (AbstractRelationship rel : subgraph.getRelationships()) {
            AbstractNode startNode = rel.getStartNode();
            AbstractNode endNode = rel.getEndNode();
            if (startNode.isNamed() && endNode.isNamed()) {
                String startNodeName = startNode.getName();
                if (!this.relationships.containsKey(startNodeName)) {
                    this.relationships.put(startNodeName, new ArrayList<AbstractRelationship>());
                }
                this.relationships.get(startNodeName).add(rel);
            }
        }
    }

    public void run() {
        for (String comment : subgraph.getComments()) {
            for (String line : comment.split("\\n")) {
                int at = line.indexOf("@");
                if (at >= 0) {
                    String[] args = line.substring(at).split(" ");
                    if (args.length >= 1) {
                        logger.info(args[0]);
                        switch (args[0]) {
                            case "@order":
                                this.assertOrder(Integer.parseInt(args[1]));
                                break;
                            case "@size":
                                this.assertSize(Integer.parseInt(args[1]));
                                break;
                            case "@node":
                                String nodeName = args[1];
                                this.assertNode(nodeName);
                                for (int i = 2; i < args.length; i++) {
                                    String arg = args[i];
                                    if (arg.startsWith(":")) {
                                        int bang = arg.indexOf("!");
                                        if (bang >= 0) {
                                            this.assertNodeIsUnique(nodeName, arg.substring(1, bang), arg.substring(bang + 1));
                                        } else {
                                            this.assertNodeLabel(nodeName, arg.substring(1));
                                        }
                                    } else {
                                        int eq = arg.indexOf("=");
                                        if (eq >= 0) {
                                            this.assertNodeProperty(nodeName, arg.substring(0, eq), arg.substring(eq + 1));
                                        } else {
                                            this.assertNodeProperty(nodeName, arg, null);
                                        }
                                    }
                                }
                                break;
                            case "@rel":
                                String startNodeName = args[1];
                                String relType;
                                boolean unique;
                                if (args[2].endsWith("!")) {
                                    relType = args[2].substring(0, args[2].length() - 1);
                                    unique = true;
                                } else {
                                    relType = args[2];
                                    unique = false;
                                }
                                String endNodeName = args[3];
                                AbstractRelationship rel = this.assertRelationship(startNodeName, relType, endNodeName);
                                if (unique) {
                                    this.assertRelationshipIsUnique(rel);
                                }
                                for (int i = 4; i < args.length; i++) {
                                    String arg = args[i];
                                    int eq = arg.indexOf("=");
                                    if (eq >= 0) {
                                        this.assertRelationshipProperty(rel, arg.substring(0, eq), arg.substring(eq + 1));
                                    } else {
                                        this.assertRelationshipProperty(rel, arg, null);
                                    }
                                }
                                break;
                        }
                    }
                }
            }
        }
    }

    public void assertOrder(int order) {
        String stringOrder = Integer.toString(order);
        System.out.println("assert order " + stringOrder);
        if (this.subgraph.order() != order) {
            throw new AssertionError("Subgraph does not have order " + stringOrder);
        }
    }

    public void assertSize(int size) {
        String stringSize = Integer.toString(size);
        System.out.println("assert size " + stringSize);
        if (this.subgraph.size() != size) {
            throw new AssertionError("Subgraph does not have size " + stringSize);
        }
    }

    public void assertHook(String hookName) {
        System.out.println("assert hook " + hookName);
        if (!this.nodes.containsKey(hookName) || this.nodes.get(hookName).getUniqueLabel() == null) {
            throw new AssertionError("Subgraph does not contain hook \"" + hookName + "\"");
        }
    }

    public void assertNode(String nodeName) {
        System.out.println("assert node " + nodeName);
        if (!this.nodes.containsKey(nodeName)) {
            throw new AssertionError("Subgraph does not contain node \"" + nodeName + "\"");
        }
    }

    public void assertNodeIsUnique(String nodeName, String label, String key) {
        System.out.println("assert node " + nodeName + " is unique for label " + label + " and key " + key);
        AbstractNode node = this.nodes.get(nodeName);
        if (!node.isUnique()) {
            throw new AssertionError("Node \"" + nodeName + "\" is not unique");
        }
        if (!label.equals(node.getUniqueLabel())) {
            throw new AssertionError("Node \"" + nodeName + "\" does not have unique label " + label);
        }
        if (!key.equals(node.getUniqueKey())) {
            throw new AssertionError("Node \"" + nodeName + "\" does not have unique key " + key);
        }
    }

    public void assertNodeLabel(String nodeName, String label) {
        System.out.println("assert node " + nodeName + " has label " + label);
        Set<String> labels = this.nodes.get(nodeName).getLabels();
        if (labels == null) {
            labels = new HashSet<>();
        }
        if (!labels.contains(label)) {
            throw new AssertionError("Node \"" + nodeName + "\" does not have label \"" + label + "\"");
        }
    }

    public void assertNodeProperty(String nodeName, String key, Object value) {
        Map<String, Object> properties = this.nodes.get(nodeName).getProperties();
        if (properties == null) {
            properties = new HashMap<>();
        }
        if (value == null) {
            System.out.println("assert node property " + nodeName + "." + key + " is null");
            if (properties.containsKey(key) && properties.get(key) != null) {
                throw new AssertionError("Node \"" + nodeName + "\" does not have property \"" + key + "\" with value null");
            }
        } else {
            String valueString = value.toString();
            System.out.println("assert node property " + nodeName + "." + key + " is " + valueString);
            if (!properties.containsKey(key) || !properties.get(key).toString().equals(valueString)) {
                throw new AssertionError("Node \"" + nodeName + "\" does not have property \"" + key + "\" with value \"" + valueString + "\"");
            }
        }
    }

    public AbstractRelationship assertRelationship(String startNodeName, String relType, String endNodeName) {
        String relString = startNodeName + " " + relType + " " + endNodeName;
        System.out.println("assert rel " + relString);
        if (this.relationships.containsKey(startNodeName)) {
            for (AbstractRelationship rel : this.relationships.get(startNodeName)) {
                if (relType.equals(rel.getType()) && endNodeName.equals(rel.getEndNode().getName())) {
                    return rel;
                }
            }
        }
        throw new AssertionError("Subgraph does not contain relationship \"" + relString + "\"");
    }

    public void assertRelationshipIsUnique(AbstractRelationship rel) {
        String relString = rel.toString();
        System.out.println("assert rel " + relString + " is unique");
        if (!rel.isUnique()) {
            throw new AssertionError("Relationship \"" + relString + "\" is not unique");
        }
    }

    public void assertRelationshipProperty(AbstractRelationship rel, String key, Object value) {
        String relString = rel.toString();
        Map<String, Object> properties = rel.getProperties();
        if (properties == null) {
            properties = new HashMap<>();
        }
        if (value == null) {
            System.out.println("assert rel property " + key + " of " + relString + " is null");
            if (properties.containsKey(key) && properties.get(key) != null) {
                throw new AssertionError("Relationship \"" + relString + "\" does not have property \"" + key + "\" with value null");
            }
        } else {
            String valueString = value.toString();
            System.out.println("assert rel property " + key + " of " + relString + " is " + valueString);
            if (!properties.containsKey(key) || !properties.get(key).toString().equals(valueString)) {
                throw new AssertionError("Relationship \"" + relString + "\" does not have property \"" + key + "\" with value \"" + valueString + "\"");
            }
        }
    }

}
