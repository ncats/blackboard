package blackboard.graphql.test;

import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.rules.ExternalResource;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.StaticDataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

public class TestGraphQL {
    static final Logger logger =
        Logger.getLogger(TestGraphQL.class.getName());

    @Rule public TestName name = new TestName();    
    public TestGraphQL () {
    }

    @Test
    public void testSimple () throws Exception {
       String schema = "type Query{hello: String}";

        SchemaParser schemaParser = new SchemaParser();
        TypeDefinitionRegistry typeDefinitionRegistry =
            schemaParser.parse(schema);

        RuntimeWiring runtimeWiring = newRuntimeWiring()
                .type("Query", builder -> builder.dataFetcher
                      ("hello", new StaticDataFetcher("world")))
            .build();
        
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema
            (typeDefinitionRegistry, runtimeWiring);

        GraphQL build = GraphQL.newGraphQL(graphQLSchema).build();
        ExecutionResult executionResult = build.execute("{hello}");

        String result = executionResult.getData().toString();
        logger.info("GraphQL => "+result);
        assertTrue ("GraphQL fails!", "{hello=world}".equals(result));
    }
}
