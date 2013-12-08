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

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AbstractRelationship {

    private static ObjectMapper mapper = new ObjectMapper();

    private AbstractNode startNode;
    private String type;
    private HashMap<String, Object> properties;
    private AbstractNode endNode;
    private boolean unique;
    private String uniqueKey;

    public AbstractRelationship(AbstractNode startNode, String type, Map<String, Object> properties, AbstractNode endNode) {
        this.startNode = startNode;
        this.type = type;
        this.mergeProperties(properties);
        this.endNode = endNode;
    }

    public AbstractRelationship(AbstractNode startNode, String type, Map<String, Object> properties, AbstractNode endNode, boolean unique) {
        this(startNode, type, properties, endNode);
        this.unique = unique;
    }

    public AbstractRelationship(AbstractNode startNode, String type, Map<String, Object> properties, AbstractNode endNode, String uniqueKey) {
        this(startNode, type, properties, endNode, true);
        this.uniqueKey = uniqueKey;
    }

    public String toString() {
        ArrayList<String> parts = new ArrayList<>();
        parts.add("(");
        parts.add(this.startNode.getName());
        parts.add(")-[:");
        parts.add(this.type);
        if (this.unique) {
            parts.add("!");
            if (this.uniqueKey != null) {
                parts.add(this.uniqueKey);
            }
        }
        if (this.properties != null) {
            if (parts.size() > 0) {
                parts.add(" ");
            }
            try {
                parts.add(mapper.writeValueAsString(this.properties));
            } catch (IOException e) {
                //
            }
        }
        parts.add("]->(");
        parts.add(this.endNode.getName());
        parts.add(")");
        return StringUtils.join(parts, "");
    }

    public AbstractNode getStartNode() {
        return this.startNode;
    }

    public String getType() {
        return this.type;
    }

    public Map<String, Object> getProperties() {
        return this.properties;
    }

    public void mergeProperties(Map<String, Object> properties) {
        if (properties != null) {
            if (this.properties == null) {
                this.properties = new HashMap<>(properties);
            } else {
                this.properties.putAll(properties);
            }
        }
    }

    public AbstractNode getEndNode() {
        return this.endNode;
    }

    public boolean isUnique() {
        return this.unique;
    }

    public String getUniqueKey() {
        return this.uniqueKey;
    }

    public Object getUniqueValue() {
        if (this.properties.containsKey(uniqueKey)) {
            return this.properties.get(uniqueKey);
        } else {
            return null;
        }
    }

}
