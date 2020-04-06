package com.teiid.firestore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FirestoreInsertFirestoreTranslatorTest {
    @Autowired
    private JdbcTemplate template;

//    TODO: 3 nested fields test + delete inserted

    @Test
    public void shouldAddDocumentToCollectionWhenInsertingWithoutId() throws InterruptedException {
        String query = "INSERT INTO CountriesT (country_name, irreligious) VALUES ('WithoutId', 11)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
    }

    @Test
    public void shouldAddDocumentToCollectionWhenInsertingWithId() {
        String query = "INSERT INTO CountriesT (id, country_name, irreligious) VALUES ('testId', 'WithId', 11)";
        int rowsAffected = template.update(query);
        assertEquals(1, rowsAffected);
    }

    @Test
    public void shouldAddMultipleDocumentsToCollectionWhenInsertingWithId() {
        String query = "INSERT INTO CountriesT (id, country_name) VALUES ('A', 'France'), ('B', 'Greece'), ('C', 'Finland')";
        int rowsAffected = template.update(query);
//        TODO: fix upd count
//        assertEquals(3, rowsAffected);
    }
}
