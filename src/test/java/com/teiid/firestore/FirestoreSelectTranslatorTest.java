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
    public void shouldReturnFilteredDocumentWhenSelectingWithArrayContainsCondition() {
        String query = "SELECT * FROM CountriesT WHERE array_contains(country_languages, 'Spanish') = true";
        List<Map<String, Object>> result = template.queryForList(query);
        assertArrayEquals(new String[]{"Spain", "TestOrder"}, result.stream().map(m -> m.get("country_name")).sorted().toArray());
    }

    @Test
    public void shouldReturnFilteredDocumentWhenSelectingWithArrayContainsAnyCondition() {
        String query = "SELECT * FROM CountriesT WHERE array_contains_any(country_languages, ARRAY['Ukrainian', 'Catalan']) = true";
        List<Map<String, Object>> result = template.queryForList(query);
        assertArrayEquals(new String[]{"Spain", "Ukraine"}, result.stream().map(m -> m.get("country_name")).sorted().toArray());
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

    @Test
    public void shouldSelectGroupIncludingParentIdFieldWhenSelectingFromSubCollection() {
        String query = "SELECT * FROM CitiesT";
        List<Map<String, Object>> result = template.queryForList(query);
        assertArrayEquals(new String[]{"Dnipro", "Lviv", "Malaga", "New York", "Sevilla", "Valencia"},
                result.stream().map(m -> m.get("city_name")).sorted().toArray());
        assertArrayEquals(new String[]{"TOywf0YvvNeMcY23k3e2", "fLrLLOR18LLYEu4Zf9pp", "nf7JODgYVpyqyVWdkHng"},
                result.stream().map(m -> m.get("parent_id")).distinct().sorted().toArray());
    }


    //    TODO: WHERE id= does not work
    @Test
    public void shouldSelectSpecificSubCollectionWhenSettingParentIdEqualityCondition() {
        String query = "SELECT city_name FROM CitiesT WHERE parent_id = 'fLrLLOR18LLYEu4Zf9pp'";
        List<Map<String, Object>> result = template.queryForList(query);
        assertArrayEquals(new String[]{"Dnipro", "Lviv"}, result.stream().map(m -> m.get("city_name")).sorted().toArray());
        assertEquals(1, result.stream().map(m -> m.get("parent_id")).distinct().count());
    }

    @Test
    public void shouldSelectSpecificSubCollectionWhenSettingParentIdLikeCondition() {
        String query = "SELECT id FROM CitiesT WHERE parent_id LIKE 'n%'";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals("i5zJ8Fbs6JV6PJkEI7B1", result.get(0).get("id"));
    }

    @Test
    public void shouldSelectSpecificSubCollectionsWhenSettingParentIdInCondition() {
        String query = "SELECT id FROM CitiesT WHERE parent_id IN ('nf7JODgYVpyqyVWdkHng', 'fLrLLOR18LLYEu4Zf9pp')";
        assertEquals(3, template.queryForList(query).size());
    }

    @Test
    public void shouldLimitedSubCollectionsWhenSettingLimit() {
        String query = "SELECT id FROM CitiesT WHERE parent_id IN ('nf7JODgYVpyqyVWdkHng', 'fLrLLOR18LLYEu4Zf9pp') LIMIT 2";
        assertEquals(2, template.queryForList(query).size());
    }

    @Test
    public void shouldSelectSpecificSubCollectionsWhenSettingCompoundCondition() {
        String query = "SELECT city_name FROM CitiesT WHERE parent_id = 'fLrLLOR18LLYEu4Zf9pp' AND population > 900";
        List<Map<String, Object>> result = template.queryForList(query);
        assertEquals("Dnipro", result.get(0).get("city_name"));
    }
}