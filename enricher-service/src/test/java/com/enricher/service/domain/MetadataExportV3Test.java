package com.enricher.service.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataExportV3Test {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void readsCalculatedFlagFromV3MainFields() throws Exception {
        MetadataExportV3 metadata = objectMapper.readValue("""
                {
                  "classes": [
                    {
                      "name": "TRADE",
                      "sourceValue": "Trade",
                      "isAbstract": false,
                      "rootType": "TRADE",
                      "mainTable": "trade_main",
                      "columnsTable": "trade_columns",
                      "dataTable": "trade_data"
                    }
                  ],
                  "fields": {
                    "TRADE": {
                      "mainFields": [
                        {"name":"tradeId","db":"trade_id","type":"LONG","calculated":false},
                        {"name":"savedAt","db":"saved_at","type":"DATETIME","calculated":true}
                      ],
                      "columnsFields": [],
                      "declaredFields": []
                    }
                  },
                  "hierarchy": {
                    "parentsOrSelf": {"TRADE":["TRADE"]},
                    "rootUnderBase": {"TRADE":"TRADE"},
                    "actualClassesForRoot": {"TRADE":["TRADE"]}
                  },
                  "columnsSources": {"TRADE":[]},
                  "relations": {"TRADE":[]},
                  "enumTypes": {}
                }
                """, MetadataExportV3.class);

        assertThat(metadata.fields().get("TRADE").mainFields())
                .extracting(MetadataExportV3.MainFieldConfig::calculated)
                .containsExactly(false, true);
        assertThat(metadata.classes().getFirst().columnsTable()).isEqualTo("trade_columns");
    }
}
