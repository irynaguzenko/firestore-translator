package com.teiid.firestore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

    @Test
    public void shouldDeleteSingleDocumentWhenDeletingById() {
        String insert = "INSERT INTO CountriesT (id) VALUES ('testDeleted')";
        template.update(insert);
        String delete = "DELETE FROM CountriesT WHERE id = 'testDeleted'";
        int rowsAffected = template.update(delete);
        assertEquals(1, rowsAffected);
        List<Map<String, Object>> maps = template.queryForList("SELECT * FROM CountriesT WHERE id = 'testDeleted'");
        assertEquals(0, maps.size());
    }

}
