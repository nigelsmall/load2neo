/*
 * Copyright 2013-2014, Nigel Small
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/all_the_things")
public class AllTheThingsResource {

    private final GraphDatabaseService database;

    public AllTheThingsResource(@Context GraphDatabaseService database) {
        this.database = database;
    }

    @DELETE
    public Response deleteAllTheThings() {

        try (Transaction tx = database.beginTx()) {

            GlobalGraphOperations global = GlobalGraphOperations.at(database);

            for (Relationship rel : global.getAllRelationships()) {
                rel.delete();
            }

            for (Node node : global.getAllNodes()) {
                node.delete();
            }

            tx.success();

        }

        return Response.status(Response.Status.NO_CONTENT).build();

    }

}
