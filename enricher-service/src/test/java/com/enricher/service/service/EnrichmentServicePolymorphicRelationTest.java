package com.enricher.service.service;

import com.enricher.service.domain.MetadataExportV3;
import com.enricher.service.registry.FieldRegistry;
import com.enricher.service.registry.ObjectClassHierarchyRegistry;
import com.enricher.service.registry.ObjectClassRegistry;
import com.enricher.service.registry.RelationRegistry;
import com.enricher.service.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class EnrichmentServicePolymorphicRelationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StubSearchServiceClient searchServiceClient = new StubSearchServiceClient();
    private final EnrichmentService enrichmentService = createService();

    @Test
    void projectsFieldDeclaredByRelationTargetDescendant() throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));
        searchServiceClient.respond("Actor", 2L, objectMapper.readTree("""
                {"objectClass":"Person","firstName":"Ivan"}
                """));

        JsonNode result = enrichmentService.enrich("Trade", 1L, List.of("Trade.trader.firstName"));

        assertThat(result).isEqualTo(objectMapper.readTree("""
                {"objectClass":"Trade","trader":{"firstName":"Ivan"}}
                """));
    }

    @Test
    void returnsNullWhenActualRelationTargetDoesNotHaveDescendantField() throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));
        searchServiceClient.respond("Actor", 2L, objectMapper.readTree("""
                {"objectClass":"Actor"}
                """));

        JsonNode result = enrichmentService.enrich("Trade", 1L, List.of("Trade.trader.firstName"));

        assertThat(result.path("trader").path("firstName").isNull()).isTrue();
    }

    @Test
    void skipsInvalidOutputFieldAndProjectsValidField(CapturedOutput output) throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));
        searchServiceClient.respond("Actor", 2L, objectMapper.readTree("""
                {"objectClass":"Person","firstName":"Ivan"}
                """));

        JsonNode result = enrichmentService.enrich(
                "Trade",
                1L,
                List.of("Trade.unknownField", "Trade.trader.firstName")
        );

        assertThat(result).isEqualTo(objectMapper.readTree("""
                {"objectClass":"Trade","trader":{"firstName":"Ivan"}}
                """));
        assertThat(output).contains("Skipping invalid outputField")
                .contains("selector=[Trade.unknownField]")
                .contains("Unknown field in outputField path");
    }

    @Test
    void returnsOnlyObjectClassWhenAllOutputFieldsAreInvalid(CapturedOutput output) throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));

        JsonNode result = enrichmentService.enrich(
                "Trade",
                1L,
                List.of("Trade.unknownField", "UnknownSource.id")
        );

        assertThat(result).isEqualTo(objectMapper.readTree("""
                {"objectClass":"Trade"}
                """));
        assertThat(output).contains("selector=[Trade.unknownField]")
                .contains("selector=[UnknownSource.id]");
    }

    @Test
    void prefersNarrowerSelectorWhenFullRelationConflictsWithNestedField(CapturedOutput output) throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));
        searchServiceClient.respond("Actor", 2L, objectMapper.readTree("""
                {"objectClass":"Person","firstName":"Ivan"}
                """));

        JsonNode result = enrichmentService.enrich(
                "Trade",
                1L,
                List.of("Trade.trader", "Trade.trader.firstName")
        );

        assertThat(result.path("trader")).isEqualTo(objectMapper.readTree("""
                {"firstName":"Ivan"}
                """));
        assertThat(output).contains("Skipping conflicting outputField")
                .contains("selector=[Trade.trader]")
                .contains("Cannot request both full relation and nested fields");
    }

    @Test
    void acceptsFieldDeclaredByMultipleDescendants() {
        MetadataExportV3 metadata = metadata();
        ObjectClassRegistry classRegistry = new ObjectClassRegistry(metadata);
        ObjectClassHierarchyRegistry hierarchyRegistry = new ObjectClassHierarchyRegistry(metadata, classRegistry);
        FieldRegistry fieldRegistry = new FieldRegistry(metadata, classRegistry, hierarchyRegistry);

        assertThat(fieldRegistry.hasFieldInPolymorphicHierarchy(
                classRegistry.byName("ACTOR"), "firstName"
        )).isTrue();
    }

    @Test
    void resolvesNestedSiblingRelationByActualClass() throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));
        searchServiceClient.respond("Actor", 2L, objectMapper.readTree("""
                {"objectClass":"Person","contactId":3}
                """));
        searchServiceClient.respond("PersonContact", 3L, objectMapper.readTree("""
                {"objectClass":"PersonContact","label":"personal"}
                """));

        JsonNode result = enrichmentService.enrich(
                "Trade", 1L, List.of("Trade.trader.contact.label")
        );

        assertThat(result.path("trader").path("contact").path("label").asText())
                .isEqualTo("personal");
    }

    @Test
    void resolvesNestedSiblingRelationByLegacyObjectType() throws Exception {
        searchServiceClient.respond("Trade", 1L, objectMapper.readTree("""
                {"objectClass":"Trade","traderId":2}
                """));
        searchServiceClient.respond("Actor", 2L, objectMapper.readTree("""
                {"objectClass":"LegacyActor","objectType":"Person","contactId":3}
                """));
        searchServiceClient.respond("PersonContact", 3L, objectMapper.readTree("""
                {"objectClass":"PersonContact","label":"personal"}
                """));

        JsonNode result = enrichmentService.enrich(
                "Trade", 1L, List.of("Trade.trader.contact.label")
        );

        assertThat(result.path("trader").path("contact").path("label").asText())
                .isEqualTo("personal");
    }

    @Test
    void warnsAndUsesDescendantRelationOnHierarchyTokenCollision(CapturedOutput output) {
        MetadataExportV3 baseMetadata = metadata();
        Map<String, List<MetadataExportV3.RelationConfig>> relations = new HashMap<>(baseMetadata.relations());
        relations.put("ACTOR", List.of(globalLink("contact", "actorContactId", "COMPANY_CONTACT")));
        MetadataExportV3 metadata = new MetadataExportV3(
                baseMetadata.classes(),
                baseMetadata.fields(),
                baseMetadata.hierarchy(),
                baseMetadata.columnsSources(),
                relations,
                baseMetadata.enumTypes()
        );
        ObjectClassRegistry classRegistry = new ObjectClassRegistry(metadata);
        ObjectClassHierarchyRegistry hierarchyRegistry = new ObjectClassHierarchyRegistry(metadata, classRegistry);

        RelationRegistry registry = new RelationRegistry(metadata, classRegistry, hierarchyRegistry);

        assertThat(output).contains("Relation token collision in class hierarchy")
                .contains("token=[contact]")
                .contains("descendantClass=[Person]")
                .contains("ancestorClass=[Actor]");
        assertThat(registry.resolveInHierarchy(classRegistry.byName("PERSON"), "contact")
                .targetClass().sourceValue()).isEqualTo("PersonContact");
    }

    private EnrichmentService createService() {
        MetadataExportV3 metadata = metadata();
        ObjectClassRegistry classRegistry = new ObjectClassRegistry(metadata);
        ObjectClassHierarchyRegistry hierarchyRegistry = new ObjectClassHierarchyRegistry(metadata, classRegistry);
        FieldRegistry fieldRegistry = new FieldRegistry(metadata, classRegistry, hierarchyRegistry);
        RelationRegistry relationRegistry = new RelationRegistry(metadata, classRegistry, hierarchyRegistry);
        return new EnrichmentService(
                searchServiceClient,
                classRegistry,
                hierarchyRegistry,
                fieldRegistry,
                relationRegistry,
                new JsonUtils(objectMapper),
                new SimpleMeterRegistry()
        );
    }

    private MetadataExportV3 metadata() {
        List<MetadataExportV3.ClassConfig> classes = List.of(
                classConfig("TRADE", "Trade", false),
                classConfig("ACTOR", "Actor", true),
                classConfig("PERSON", "Person", false),
                classConfig("EMPLOYEE", "Employee", false),
                classConfig("COMPANY", "Company", false),
                classConfig("PERSON_CONTACT", "PersonContact", false),
                classConfig("COMPANY_CONTACT", "CompanyContact", false)
        );
        Map<String, MetadataExportV3.FieldsConfig> fields = Map.of(
                "TRADE", fields("traderId"),
                "ACTOR", fields(),
                "PERSON", fields("firstName"),
                "EMPLOYEE", fields("firstName"),
                "COMPANY", fields(),
                "PERSON_CONTACT", fields("label"),
                "COMPANY_CONTACT", fields("label")
        );
        MetadataExportV3.HierarchyConfig hierarchy = new MetadataExportV3.HierarchyConfig(
                Map.of(
                        "TRADE", List.of("TRADE"),
                        "ACTOR", List.of("ACTOR"),
                        "PERSON", List.of("PERSON", "ACTOR"),
                        "EMPLOYEE", List.of("EMPLOYEE", "ACTOR"),
                        "COMPANY", List.of("COMPANY", "ACTOR"),
                        "PERSON_CONTACT", List.of("PERSON_CONTACT"),
                        "COMPANY_CONTACT", List.of("COMPANY_CONTACT")
                ),
                Map.of(),
                Map.of()
        );
        MetadataExportV3.RelationConfig trader = new MetadataExportV3.RelationConfig(
                "traderRelation", "trader", "GLOBAL_LINK", "ACTOR",
                "traderId", "traderId", null, null, null, null, null,
                null, null, null
        );
        return new MetadataExportV3(
                classes,
                fields,
                hierarchy,
                Map.of(),
                Map.of(
                        "TRADE", List.of(trader),
                        "PERSON", List.of(globalLink("contact", "contactId", "PERSON_CONTACT")),
                        "COMPANY", List.of(globalLink("contact", "contactId", "COMPANY_CONTACT"))
                ),
                Map.of()
        );
    }

    private MetadataExportV3.RelationConfig globalLink(String alias, String sourceJsonName, String targetClass) {
        return new MetadataExportV3.RelationConfig(
                sourceJsonName, alias, "GLOBAL_LINK", targetClass,
                sourceJsonName, sourceJsonName, null, null, null, null, null,
                null, null, null
        );
    }

    private MetadataExportV3.ClassConfig classConfig(String name, String sourceValue, boolean isAbstract) {
        return new MetadataExportV3.ClassConfig(name, sourceValue, isAbstract, null, null, null, null);
    }

    private MetadataExportV3.FieldsConfig fields(String... names) {
        List<MetadataExportV3.DeclaredFieldConfig> declared = List.of(names).stream()
                .map(name -> new MetadataExportV3.DeclaredFieldConfig(name, null, null))
                .toList();
        return new MetadataExportV3.FieldsConfig(List.of(), List.of(), declared);
    }

    private static final class StubSearchServiceClient extends SearchServiceClient {
        private final Map<String, JsonNode> responses = new HashMap<>();

        private StubSearchServiceClient() {
            super(null, null, null, null);
        }

        private void respond(String objectClass, long globalId, JsonNode response) {
            responses.put(key(objectClass, globalId), response);
        }

        @Override
        public JsonNode getObjectByGlobalId(String objectClass, long globalId) {
            return responses.get(key(objectClass, globalId));
        }

        private String key(String objectClass, long globalId) {
            return objectClass + "|" + globalId;
        }
    }
}
