package com.teiid.firestore;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class FirestoreTranslatorApplication implements CommandLineRunner {

    private final JdbcTemplate jdbcTemplate;

    public FirestoreTranslatorApplication(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(FirestoreTranslatorApplication.class, args);
    }

    @Override
    public void run(String... args) {
//        List<Map<String, Object>> list = jdbcTemplate.queryForList("SELECT country_name FROM CountriesT WHERE country_name = 'Spain' AND country_area >= 505.99 AND country_area < 1000");
        List<Map<String, Object>> list = jdbcTemplate.queryForList("SELECT country_name FROM CountriesT WHERE irreligious > 20");
        System.out.println(list);
    }
}
