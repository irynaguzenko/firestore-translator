package com.teiid.firestore;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FirestoreInsertTranslatorTest {
    @Autowired
    private JdbcTemplate template;

    @After
    public void deleteCreated() {
        String query = "DELETE from CountriesT WHERE test";
        template.update(query);
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
}
