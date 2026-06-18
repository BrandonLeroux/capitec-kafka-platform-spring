package com.capitec.kafka.orderservice.repository;

import com.capitec.kafka.orderservice.model.Customer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import jakarta.annotation.PostConstruct;
import java.util.List;

@Repository
public class CustomerRepository {

    private final JdbcTemplate jdbc;

    public CustomerRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void init() {
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id     TEXT PRIMARY KEY,
                customer_number BIGINT,
                first_name      TEXT,
                last_name       TEXT,
                id_number       TEXT,
                email           TEXT,
                cell            TEXT UNIQUE,
                password_hash   TEXT,
                created_at      TEXT DEFAULT (datetime('now'))
            )""");
        for (String col : new String[]{
            "ALTER TABLE customers ADD COLUMN customer_number BIGINT",
            "ALTER TABLE customers ADD COLUMN id_number TEXT",
            "ALTER TABLE customers ADD COLUMN password_hash TEXT"
        }) { try { jdbc.execute(col); } catch (Exception ignored) {} }
    }

    public synchronized void upsert(Customer c) {
        jdbc.update("""
            INSERT INTO customers (customer_id,customer_number,first_name,last_name,id_number,email,cell,password_hash,created_at)
            VALUES (?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(customer_id) DO UPDATE SET
                customer_number = CASE WHEN excluded.customer_number > 0 THEN excluded.customer_number ELSE customer_number END,
                first_name      = excluded.first_name,
                last_name       = excluded.last_name,
                id_number       = COALESCE(excluded.id_number, id_number),
                email           = excluded.email,
                cell            = excluded.cell,
                password_hash   = COALESCE(excluded.password_hash, password_hash)
            """,
            c.customerID, c.customerNumber, c.firstName, c.lastName,
            c.idNumber, c.email, c.cell, c.passwordHash);
    }

    public Customer findById(String id) {
        var list = jdbc.query("SELECT * FROM customers WHERE customer_id=?", ROW_MAPPER, id);
        return list.isEmpty() ? null : list.get(0);
    }

    public Customer findByIdentifier(String value) {
        var list = jdbc.query(
            "SELECT * FROM customers WHERE cell=? OR email=? OR CAST(customer_number AS TEXT)=?",
            ROW_MAPPER, value, value, value);
        return list.isEmpty() ? null : list.get(0);
    }

    public long maxCustomerNumber() {
        Long max = jdbc.queryForObject("SELECT MAX(customer_number) FROM customers", Long.class);
        return max != null ? max : 999_999_999L;
    }

    public List<Customer> findAll(String search, int limit, int offset) {
        var sql = new StringBuilder("SELECT * FROM customers WHERE 1=1");
        var args = new java.util.ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (customer_id LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR cell LIKE ?)");
            String like = "%" + search + "%";
            for (int i = 0; i < 5; i++) args.add(like);
        }
        sql.append(" ORDER BY created_at DESC LIMIT ? OFFSET ?");
        args.add(limit); args.add(offset);
        return jdbc.query(sql.toString(), ROW_MAPPER, args.toArray());
    }

    public int count(String search) {
        var sql = new StringBuilder("SELECT COUNT(*) FROM customers WHERE 1=1");
        var args = new java.util.ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (customer_id LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR cell LIKE ?)");
            String like = "%" + search + "%";
            for (int i = 0; i < 5; i++) args.add(like);
        }
        return jdbc.queryForObject(sql.toString(), Integer.class, args.toArray());
    }

    private static final RowMapper<Customer> ROW_MAPPER = (rs, i) -> {
        var c = new Customer();
        c.customerID     = rs.getString("customer_id");
        c.customerNumber = rs.getLong("customer_number");
        c.firstName      = rs.getString("first_name");
        c.lastName       = rs.getString("last_name");
        c.idNumber       = rs.getString("id_number");
        c.email          = rs.getString("email");
        c.cell           = rs.getString("cell");
        c.passwordHash   = rs.getString("password_hash");
        c.createdAt      = rs.getString("created_at");
        return c;
    };
}
