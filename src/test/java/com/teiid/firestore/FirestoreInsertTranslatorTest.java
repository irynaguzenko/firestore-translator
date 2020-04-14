package com.teiid.firestore;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import org.teiid.core.types.ArrayImpl;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FirestoreInsertTranslatorTest {
    @Autowired
    private JdbcTemplate template;

    @After
    public void deleteCreated() {
        String deleteCountries = "DELETE from CountriesT WHERE test";
        template.update(deleteCountries);
        String deleteCities = "DELETE from CitiesT WHERE test";
        template.update(deleteCities);
    }

    @Test
    public void shouldAddDocumentToCollectionWhenInsertingWithoutId() {
        String query = "INSERT INTO CountriesT (country_name, irreligious, test) VALUES ('WithoutId', 11, true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        assertEquals(1, template.queryForList("SELECT * FROM CountriesT WHERE country_name = 'WithoutId'").size());
    }

    @Test
    public void shouldAddDocumentToCollectionWhenInsertingWithId() {
        String query = "INSERT INTO CountriesT (id, country_name, test) VALUES ('testId', 'WithId', true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        assertEquals(1, template.queryForList("SELECT * FROM CountriesT WHERE id = 'testId'").size());
    }

    @Test
    public void shouldAddDocumentToCollectionWhenItContainsNestedFieldName() {
        String query = "INSERT INTO CountriesT (president_name, test) VALUES ('Frank-Walter Steinmeier', true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        assertEquals(1, template.queryForList("SELECT * FROM CountriesT WHERE president_name = 'Frank-Walter Steinmeier'").size());
    }

    @Test
    public void shouldAddMultipleDocumentsToCollectionWhenInsertingWithId() {
        String query = "INSERT INTO CountriesT (id, country_name, test) VALUES ('A', 'France', true), ('B', 'Greece', true), ('C', 'Finland', true)";
        int rowsAffected = template.update(query);
        assertEquals(3, rowsAffected);
        assertEquals(3, template.queryForList("SELECT * FROM CountriesT WHERE country_name IN ('France', 'Greece', 'Finland')").size());
    }

    @Test
    public void shouldAddDocumentWithArrayFieldToCollectionWhenCreatingArrayOfIntType() {
        String query = "INSERT INTO CountriesT (id, int_array, test) VALUES ('arr123', ARRAY[22, 11, 0], true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> select = template.queryForList("SELECT int_array FROM CountriesT WHERE id = 'arr123'");
        assertArrayEquals(new Integer[]{22, 11, 0}, ((ArrayImpl) select.get(0).get("int_array")).getValues());
    }

    @Test
    public void shouldAddDocumentWithArrayFieldToCollectionWhenCreatingArrayOfVarcharType() {
        String query = "INSERT INTO CountriesT (id, varchar_array, test) VALUES ('arr456', ARRAY['str1', 'str2'], true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> select = template.queryForList("SELECT varchar_array FROM CountriesT WHERE id = 'arr456'");
        assertArrayEquals(new String[]{"str1", "str2"}, ((ArrayImpl) select.get(0).get("varchar_array")).getValues());
    }

    @Test
    public void shouldAddSubCollectionWhenInsertingWithParentIdAndId() {
        String query = "INSERT INTO CitiesT (id, parent_id, test) VALUES ('testCityId', '8B29ww4lnHkrbWL0XH10', true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> select = template.queryForList("SELECT id FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertEquals(1, select.size());
        assertEquals("testCityId", select.get(0).get("id"));
    }

    @Test
    public void shouldAddSubCollectionWithGeneratedIdWhenInsertingWithParentIdWithoutId() {
        String query = "INSERT INTO CitiesT (parent_id, city_name, test) VALUES ('8B29ww4lnHkrbWL0XH10', 'Granada', true)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> select = template.queryForList("SELECT city_name, parent_id FROM CitiesT WHERE city_name = 'Granada'");
        assertEquals("Granada", select.get(0).get("city_name"));
        assertEquals("8B29ww4lnHkrbWL0XH10", select.get(0).get("parent_id"));
    }

    @Test
    public void shouldAddSubCollectionsWhenInsertingMultipleValuesWithIds() {
        String query = "INSERT INTO CitiesT (id, parent_id, test) " +
                "VALUES ('test1', '8B29ww4lnHkrbWL0XH10', true), ('test2', '8B29ww4lnHkrbWL0XH10', true), ('test3', '8B29ww4lnHkrbWL0XH10', true)";
        int rowsAffected = template.update(query);
        assertEquals(3, rowsAffected);
        List<Map<String, Object>> select = template.queryForList("SELECT id FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertArrayEquals(new String[]{"test1", "test2", "test3"}, select.stream().map(m -> m.get("id")).sorted().toArray());
    }

    @Test
    public void shouldAddSubCollectionsWhenInsertingMultipleValuesWithoutIds() {
        String query = "INSERT INTO CitiesT (parent_id, city_name, test) " +
                "VALUES ('8B29ww4lnHkrbWL0XH10', 'A', true), ('8B29ww4lnHkrbWL0XH10', 'B', true), ('8B29ww4lnHkrbWL0XH10', 'C', true)";
        int rowsAffected = template.update(query);
        assertEquals(3, rowsAffected);
        List<Map<String, Object>> select = template.queryForList("SELECT city_name, parent_id FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertArrayEquals(new String[]{"A", "B", "C"}, select.stream().map(m -> m.get("city_name")).sorted().toArray());
    }

    @Test(expected = UncategorizedSQLException.class)
    public void shouldThrowExceptionWhenInsertingToSubCollectionWithoutParentId() {
        String query = "INSERT INTO CitiesT (city_name, test) VALUES ('A', true), ('B', true), ('C', true)";
        template.update(query);
    }
}
