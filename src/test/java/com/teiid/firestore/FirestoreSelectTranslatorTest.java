package com.teiid.firestore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FirestoreSelectTranslatorTest {
    @Autowired
    private JdbcTemplate template;

    @Test
    public void shouldReturnDeclaredFieldsWhenSettingFieldNames() {
        String query = "SELECT country_area, right_side_driving FROM CountriesT";
        List<Map<String, Object>> result = template.queryForList(query);
        assertArrayEquals(new String[]{"country_area", "right_side_driving"}, result.get(0).keySet().toArray());
    }

    @Test
    public void shouldReturnDocumentIdsWhenSelectingReservedNameColumn() {
        String query = "SELECT id, country_name FROM CountriesT";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals("8B29ww4lnHkrbWL0XH10", result.stream().map(m -> m.get("id")).sorted().toArray()[0]);
    }

    @Test
    public void shouldReturnFilteredDocumentWhenSelectingWithMultipleWhereClauses() {
        String query = "SELECT country_name FROM CountriesT WHERE irreligious > 20 AND right_side_driving = false AND country_name = 'Spain'";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals(1, result.size());
        assertEquals("Spain", result.get(0).get("country_name"));
    }

    @Test
    public void shouldReturnDocumentsWhenSelectingWithSingleBooleanFilter() {
        String query = "SELECT country_name, right_side_driving FROM CountriesT WHERE right_side_driving";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals(1, result.size());
        assertEquals("Ukraine", result.get(0).get("country_name"));
    }

    //Document Id is not supported for 'WHERE IN' conditions
    @Test
    public void shouldReturnFilteredDocumentWhenSelectingWithInCondition() {
        String query = "SELECT * FROM CountriesT WHERE country_name IN ('Italy', 'Ukraine', 'Sweden')";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals(1, result.size());
        assertEquals("Ukraine", result.get(0).get("country_name"));
    }

    @Test
    public void shouldReturnFilteredDocumentWhenSettingTheFieldPrefixValue() {
        String query = "SELECT country_capital FROM CountriesT WHERE country_capital LIKE ('Ma%')";
        List<Map<String, Object>> result = template.queryForList(query);
        assertArrayEquals(new String[]{"Madrid", "Malaga"}, result.stream().map(m -> m.get("country_capital")).sorted().toArray());
    }

    @Test
    public void shouldReturnOrderedRecordsWhenOrderingByAreaAndName() {
        String query = "SELECT * FROM CountriesT ORDER BY country_area, country_name DESC";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals("Spain", result.get(0).get("country_name"));
        assertEquals("Ukraine", result.get(1).get("country_name"));
        assertEquals("TestOrder", result.get(2).get("country_name"));
    }

    @Test
    public void shouldReturnTwoRecordsWhenSettingLimit2() {
        String query = "SELECT * FROM CountriesT LIMIT 2";
        assertEquals(2, template.queryForList(query).size());
    }
}