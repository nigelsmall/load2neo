package com.nigelsmall.load2neo.test;

import com.nigelsmall.geoff.reader.GeoffReader;
import com.nigelsmall.load2neo.AbstractNode;
import com.nigelsmall.load2neo.NeoLoader;
import com.nigelsmall.load2neo.Subgraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NeoLoaderTest {

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();
    private GraphDatabaseService database;


    @Before
    public void setUp() {
        // create an instance of Neo4J that we can use for testing
        database = new GraphDatabaseFactory().newEmbeddedDatabase(dbFolder.getRoot().getPath());
    }

    @After
    public void tearDown()  {
        // shutdown neo4j
        if(database != null)
            database.shutdown();
    }

    @Test
    public void validate_hook_with_multiple_keys_programmatically() {

        // set up test data
        try(Transaction transaction = database.beginTx()) {

            Node ingrid10 = database.createNode(DynamicLabel.label("Person"));
            ingrid10.setProperty("name", "Ingrid");
            ingrid10.setProperty("age", 10);

            Node ingrid20 = database.createNode(DynamicLabel.label("Person"));
            ingrid20.setProperty("name", "Ingrid");
            ingrid20.setProperty("age", 20);

            Node ingrid30 = database.createNode(DynamicLabel.label("Person"));
            ingrid30.setProperty("name", "Ingrid");
            ingrid30.setProperty("age", 30);

            NeoLoader neoLoader = new NeoLoader(database);

            HashSet<String> labels = new HashSet<>();
            labels.add("Person");

            HashMap<String, Object> properties = new HashMap<>();
            properties.put("name", "Ingrid");
            properties.put("age", 10);

            AbstractNode goeffNode = new AbstractNode("Person", labels, properties);
            goeffNode.setHook("Person", Arrays.asList("name", "age"), false);

            Node foundNode = neoLoader.createOrUpdateNode(goeffNode);

            assertEquals(ingrid10, foundNode);
            assertNotEquals(ingrid20, foundNode);
            assertNotEquals(ingrid30, foundNode);
            assertEquals("Ingrid", foundNode.getProperty("name"));
            assertEquals(10, foundNode.getProperty("age"));
            assertNotNull(foundNode);

            transaction.success();

        }
    }

    @Test
    public void validate_hook_with_multiple_keys_from_geoff_source() throws IOException {

        // load test data from Goeff file
        final Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("hookstest.geoff");
        final URL url = urls.nextElement();
        final GeoffReader geoffReader = new GeoffReader(new FileReader(new File(url.getPath())));
        final Subgraph testSubgraph = geoffReader.readSubgraph();

        try(Transaction transaction = database.beginTx()) {

            NeoLoader neoLoader = new NeoLoader(database);

            // load the subgraph into neo
            Map<String, Node> result = neoLoader.load(testSubgraph);

            assertEquals(3, result.size());
            assertEquals(2, count(GlobalGraphOperations.at(database).getAllNodes()));
            assertEquals(2, count(GlobalGraphOperations.at(database).getAllRelationships()));

            Node ingrid10 = result.get("ingrid10");
            Node ingrid20 = result.get("ingrid20");
            Node ingrid30 = result.get("ingrid30");

            assertNotNull(ingrid10);
            assertNotNull(ingrid20);
            assertNull(ingrid30);

            assertTrue(hasRelationshipTo(ingrid10, ingrid20));
            assertTrue(hasRelationshipTo(ingrid20, ingrid10));

            transaction.success();
        }
    }

    private <T> int count(Iterable<T> items) {
        int count = 0;

        for(T item : items) {
            count++;
        }

        return count;
    }

    private boolean hasRelationshipTo(Node from, Node to) {

        for(Relationship relationship : from.getRelationships()) {

            if(relationship.getEndNode().getId() == to.getId())
                return true;
        }

        return false;
    }

}
