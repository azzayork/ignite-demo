package com.azzayork.ignite.jdbc;

import com.azzayork.ignite.client.IgniteClient;
import com.azzayork.ignite.domain.City;
import com.azzayork.ignite.domain.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class JdbcHelperTest {


    private IgniteCache<Long, Person> personCache;
    private IgniteCache<Long, City> cityCache;

    private JdbcHelper helper;

    @Before
    public void setUp() {

        IgniteClient igniteClient = new IgniteClient();
        Ignite ignite = igniteClient.getIgnite();

        helper = new JdbcHelper();

        helper.createCityTable();
        helper.createPersonTable();

        // Getting a reference to an underlying cache created for City table above.
        cityCache = ignite.cache("SQL_PUBLIC_CITY");

        // Getting a reference to an underlying cache created for Person table above.
        personCache = ignite.cache("SQL_PUBLIC_PERSON");
    }

    @After
    public void cleanUp() {
        helper.dropTable("City");
        helper.dropTable("Person");
    }

    @Test
    public void testTablesAreCreated() {

        // Inserting entries into City.
        SqlFieldsQuery query = new SqlFieldsQuery(
                "INSERT INTO City (id, name) VALUES (?, ?)");

        cityCache.query(query.setArgs(1, "Forest Hill")).getAll();
        cityCache.query(query.setArgs(2, "Denver")).getAll();
        cityCache.query(query.setArgs(3, "St. Petersburg")).getAll();

        // Inserting entries into Person.
        query = new SqlFieldsQuery(
                "INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)");

        personCache.query(query.setArgs(1, "John Doe", 3)).getAll();
        personCache.query(query.setArgs(2, "Jane Roe", 2)).getAll();
        personCache.query(query.setArgs(3, "Mary Major", 1)).getAll();
        personCache.query(query.setArgs(4, "Richard Miles", 2)).getAll();

        // Querying data from the cluster using a distributed JOIN.
        SqlFieldsQuery selectQuery = new SqlFieldsQuery("SELECT p.name, c.name " +
                " FROM Person p, City c WHERE p.city_id = c.id");

        FieldsQueryCursor<List<?>> cursor = cityCache.query(selectQuery);

        Iterator<List<?>> iterator = cursor.iterator();

        System.out.println("");
        while (iterator.hasNext()) {
            List<?> row = iterator.next();

            System.out.println(row.get(0) + ", " + row.get(1));
        }
        System.out.println("");
    }

}