package com.teiid.firestore;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FirestoreDeleteTranslatorTest {
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
    public void shouldDeleteSingleDocumentWhenDeletingById() {
        String insert = "INSERT INTO CountriesT (id, test) VALUES ('testDeleted', true)";
        template.update(insert);
        String delete = "DELETE FROM CountriesT WHERE id = 'testDeleted'";
        int rowsAffected = template.update(delete);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> result = template.queryForList("SELECT * FROM CountriesT WHERE id = 'testDeleted'");
        assertEquals(0, result.size());
    }

    @Test
    public void shouldDeleteSubCollectionWhenDeletingByParentId() {
        String insert = "INSERT INTO CitiesT (id, parent_id, test) VALUES ('test1', '8B29ww4lnHkrbWL0XH10', true), ('test2', '8B29ww4lnHkrbWL0XH10', true)";
        template.update(insert);
        String delete = "DELETE FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'";
        int rowsAffected = template.update(delete);
        assertEquals(2, rowsAffected);
        List<Map<String, Object>> result = template.queryForList("SELECT * FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertEquals(0, result.size());
    }

    @Test
    public void shouldDeleteSubCollectionDocumentWhenDeletingByParentIdAndId() {
        String insert = "INSERT INTO CitiesT (id, parent_id, test) VALUES ('test1', '8B29ww4lnHkrbWL0XH10', true), ('test2', '8B29ww4lnHkrbWL0XH10', true)";
        template.update(insert);
        String delete = "DELETE FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10' and id = 'test2'";
        int rowsAffected = template.update(delete);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> result = template.queryForList("SELECT * FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertEquals(1, result.size());
        assertEquals("test1", result.get(0).get("id"));
    }

    @Test
    public void shouldDeleteGroupWhenDeletingWithoutParentId() {
        String insert1 = "INSERT INTO CitiesT (parent_id, city_name, test) VALUES ('8B29ww4lnHkrbWL0XH10', 'GROUP', true), ('8B29ww4lnHkrbWL0XH10', 'GROUP', true)";
        template.update(insert1);
        String insert2 = "INSERT INTO CitiesT (parent_id, city_name, test) VALUES ('nf7JODgYVpyqyVWdkHng', 'GROUP', true)";
        template.update(insert2);
        String delete = "DELETE FROM CitiesT WHERE city_name = 'GROUP'";
        int rowsAffected = template.update(delete);
        assertEquals(3, rowsAffected);
        List<Map<String, Object>> result1 = template.queryForList("SELECT * FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertEquals(0, result1.size());
        List<Map<String, Object>> result2 = template.queryForList("SELECT * FROM CitiesT WHERE parent_id = 'nf7JODgYVpyqyVWdkHng'");
        assertEquals(1, result2.size());
        assertEquals("New York", result2.get(0).get("city_name"));
    }

    @Test(expected = UncategorizedSQLException.class)
    public void shouldThrowExceptionWhenDeletingSubCollectionByIdOnly() {
        String insert = "INSERT INTO CitiesT (id, parent_id, test) VALUES ('test1', '8B29ww4lnHkrbWL0XH10', true), ('test2', '8B29ww4lnHkrbWL0XH10', true)";
        template.update(insert);
        String delete = "DELETE FROM CitiesT WHERE id = 'test2'";
        template.update(delete);
    }
}
