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
import java.util.*;

public class AbstractNode {

    private static ObjectMapper mapper = new ObjectMapper();

    private String name;
    private boolean named;
    private HashSet<String> labels;
    private HashMap<String, Object> properties;
    private String uniqueLabel;
    private String uniqueKey;

    public AbstractNode(String name, Set<String> labels, Map<String, Object> properties) {
        if (name == null) {
            this.name = UUID.randomUUID().toString();
            this.named = false;
        } else {
            this.name = name;
            this.named = true;
        }
        this.mergeLabels(labels);
        this.mergeProperties(properties);
    }

    public String toString() {
        final ArrayList<String> parts = new ArrayList<>();
        if (this.name != null && !this.name.equals("")) {
            parts.add(this.name);
        }
        if (this.labels != null) {
            String labels = "";
            for (String label : this.labels) {
                labels += ":" + label;
                if (label.equals(this.uniqueLabel)) {
                    labels += "!" + uniqueKey;
                }
            }
            parts.add(labels);
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
        return "(" + StringUtils.join(parts, "") + ")";
    }

    public String getName() {
        return this.name;
    }

    public boolean isNamed() {
        return this.named;
    }

    public boolean isUnique() {
        return this.uniqueLabel != null && this.uniqueKey != null;
    }

    public Set<String> getLabels() {
        return this.labels;
    }

    public Map<String, Object> getProperties() {
        return this.properties;
    }

    public void mergeNode(AbstractNode node) {
        if (node.name != null) {
            this.name = node.name;
        }
        this.mergeLabels(node.labels);
        this.mergeProperties(node.properties);
    }

    public void mergeLabels(Set<String> labels) {
        if (labels != null) {
            if (this.labels == null) {
                this.labels = new HashSet<>(labels);
            } else {
                this.labels.addAll(labels);
            }
        }
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

    public void setUnique(String label, String key) {
        if (label != null && key != null) {
            if (this.labels == null) {
                this.labels = new HashSet<>();
            }
            this.labels.add(label);
            this.uniqueLabel = label;
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            if (!this.properties.containsKey(key)) {
                this.properties.put(key, null);
            }
            this.uniqueKey = key;
        }
    }

    public String getUniqueLabel() {
        return this.uniqueLabel;
    }

    public String getUniqueKey() {
        return this.uniqueKey;
    }

}
