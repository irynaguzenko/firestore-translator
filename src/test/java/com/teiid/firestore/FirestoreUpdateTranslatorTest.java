package com.teiid.firestore;

import org.junit.After;
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
public class FirestoreUpdateTranslatorTest {
    @Autowired
    private JdbcTemplate template;

    @After
    public void deleteCreated() {
        String delete = "DELETE FROM CountriesT WHERE test";
        template.update(delete);
    }

    @Test
    public void shouldUpdateSingleFieldWhenUpdatingSingleDocument() {
        String insert = "INSERT INTO CountriesT (country_name, country_capital, test) VALUES ('Poland', 'Krakiv', true)";
        template.update(insert);
        String update = "UPDATE CountriesT SET country_capital = 'Warsaw' WHERE country_name = 'Poland'";
        assertEquals(1, template.update(update));
        List<Map<String, Object>> maps = template.queryForList("SELECT * FROM CountriesT WHERE country_name = 'Poland'");
        assertEquals(1, maps.size());
        assertEquals("Warsaw", maps.get(0).get("country_capital"));
    }

    @Test
    public void shouldUpdateMultipleFieldsWhenUpdatingMultipleDocuments() {
//        BigDecimal by default. DataTypeManager. SetQuery.getProjectedSymbols()
        String insert = "INSERT INTO CountriesT (country_name, country_area, test) VALUES ('Japan', CAST(377.91 as double), true), ('Jamaica', CAST(10.992 as double), true)";
        template.update(insert);
        String update = "UPDATE CountriesT SET country_area = 0 WHERE country_name LIKE 'J%'";
        assertEquals(2, template.update(update));
        List<Map<String, Object>> result = template.queryForList("SELECT country_name FROM CountriesT WHERE country_area = 0");
        assertArrayEquals(new String[]{"Jamaica", "Japan"}, result.stream().map(m -> m.get("country_name")).sorted().toArray());
    }

    @Test
    public void shouldUpdateMultipleFieldsWhenUpdatingSubCollectionWithParentId() {
        String insert = "INSERT INTO CitiesT (city_name, parent_id, test) VALUES ('Milan', '8B29ww4lnHkrbWL0XH10', true), ('Rome', '8B29ww4lnHkrbWL0XH10', true)";
        template.update(insert);
        String update = "UPDATE CitiesT SET city_name = 'Unknown' WHERE parent_id = '8B29ww4lnHkrbWL0XH10'";
        assertEquals(2, template.update(update));
        List<Map<String, Object>> result = template.queryForList("SELECT * FROM CitiesT WHERE parent_id = '8B29ww4lnHkrbWL0XH10'");
        assertArrayEquals(new String[]{"Unknown"}, result.stream().map(m -> m.get("city_name")).distinct().toArray());
    }
}
