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

import com.nigelsmall.geoff.Subgraph;
import com.nigelsmall.geoff.loader.NeoLoader;
import com.nigelsmall.geoff.reader.GeoffReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Map;

@Path("/load")
public class LoaderResource {

    private final static CharsetEncoder UTF8_ENCODER = Charset.forName("UTF-8").newEncoder();

    private final GraphDatabaseService database;

    public LoaderResource(@Context GraphDatabaseService database) {
        this.database = database;
    }

    @POST
    @Produces("text/x-tab-separated-json; charset=UTF-8")
    @Path("/geoff")
    public Response loadGeoff(Reader reader) {

        final GeoffReader geoffReader = new GeoffReader(reader);
        final NeoLoader neoLoader = new NeoLoader(database);

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException {
                Writer writer = new BufferedWriter(new OutputStreamWriter(os, UTF8_ENCODER));
                while (geoffReader.hasMore()) {
                    Subgraph subgraph = geoffReader.readSubgraph();
                    try (Transaction tx = database.beginTx()) {
                        Map<String, Node> nodes = neoLoader.load(subgraph);
                        writer.write("{");
                        String separator = "";
                        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
                            Node node = entry.getValue();
                            writer.write(separator);
                            writer.write('"');
                            writer.write(entry.getKey());
                            writer.write('"');
                            writer.write(':');
                            writer.write(Long.toString(node.getId()));
                            separator = ",";
                        }
                        writer.write("}\n");
                        writer.flush();
                        tx.success();
                    }
                }
            }

        };

        return Response.status(Response.Status.OK).entity(stream).build();

    }

}
