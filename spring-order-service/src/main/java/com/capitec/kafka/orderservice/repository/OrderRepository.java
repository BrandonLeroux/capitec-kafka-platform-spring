package com.capitec.kafka.orderservice.repository;

import com.capitec.kafka.orderservice.model.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import jakarta.annotation.PostConstruct;
import java.util.List;

@Repository
public class OrderRepository {

    private final JdbcTemplate jdbc;

    public OrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void init() {
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id            TEXT PRIMARY KEY,
                customer_id         TEXT,
                product             TEXT,
                amount              REAL,
                qty                 INTEGER DEFAULT 1,
                status              TEXT,
                cancellation_reason TEXT,
                received_at         TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now'))
            )""");
        try { jdbc.execute("ALTER TABLE orders ADD COLUMN cancellation_reason TEXT"); } catch (Exception ignored) {}
        try { jdbc.execute("ALTER TABLE orders ADD COLUMN qty INTEGER DEFAULT 1"); }   catch (Exception ignored) {}
        jdbc.execute("PRAGMA journal_mode=WAL");
        jdbc.execute("PRAGMA busy_timeout=5000");
    }

    public synchronized void upsert(Order o) {
        jdbc.update("""
            INSERT INTO orders (order_id, customer_id, product, amount, qty, status, received_at, updated_at)
            VALUES (?,?,?,?,?,?,datetime('now'),datetime('now'))
            ON CONFLICT(order_id) DO UPDATE SET
                status     = excluded.status,
                updated_at = datetime('now')
            """, o.orderID, o.customerID, o.product, o.amount, o.qty, o.status);
    }

    public synchronized void updateStatus(String orderID, String status) {
        jdbc.update("UPDATE orders SET status=?, updated_at=datetime('now') WHERE order_id=?", status, orderID);
    }

    public synchronized void updateCancelled(String orderID, String reason) {
        jdbc.update("UPDATE orders SET status='CANCELLED', cancellation_reason=?, updated_at=datetime('now') WHERE order_id=?", reason, orderID);
    }

    public synchronized void seedUpsert(Order o) {
        jdbc.update("""
            INSERT INTO orders (order_id, customer_id, product, amount, status, received_at, updated_at)
            VALUES (?,?,?,?,?,datetime('now'),datetime('now'))
            ON CONFLICT(order_id) DO UPDATE SET status=excluded.status, updated_at=datetime('now')
            """, o.orderID, o.customerID, o.product, o.amount, o.status);
    }

    public List<Order> findAll(String search, String status, String customerID, int limit, int offset) {
        var sql = new StringBuilder("SELECT * FROM orders WHERE 1=1");
        var args = new java.util.ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (order_id LIKE ? OR customer_id LIKE ? OR product LIKE ?)");
            String like = "%" + search + "%";
            args.add(like); args.add(like); args.add(like);
        }
        if (status != null && !status.isBlank() && !"ALL".equals(status)) { sql.append(" AND status=?"); args.add(status); }
        if (customerID != null && !customerID.isBlank()) { sql.append(" AND customer_id=?"); args.add(customerID); }
        sql.append(" ORDER BY updated_at DESC LIMIT ? OFFSET ?");
        args.add(limit); args.add(offset);
        return jdbc.query(sql.toString(), ROW_MAPPER, args.toArray());
    }

    public int count(String search, String status, String customerID) {
        var sql = new StringBuilder("SELECT COUNT(*) FROM orders WHERE 1=1");
        var args = new java.util.ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (order_id LIKE ? OR customer_id LIKE ? OR product LIKE ?)");
            String like = "%" + search + "%";
            args.add(like); args.add(like); args.add(like);
        }
        if (status != null && !status.isBlank() && !"ALL".equals(status)) { sql.append(" AND status=?"); args.add(status); }
        if (customerID != null && !customerID.isBlank()) { sql.append(" AND customer_id=?"); args.add(customerID); }
        return jdbc.queryForObject(sql.toString(), Integer.class, args.toArray());
    }

    public long countByStatus(String status) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE status=?", Long.class, status);
    }

    private static final RowMapper<Order> ROW_MAPPER = (rs, i) -> {
        var o = new Order();
        o.orderID            = rs.getString("order_id");
        o.customerID         = rs.getString("customer_id");
        o.product            = rs.getString("product");
        o.amount             = rs.getDouble("amount");
        o.qty                = rs.getInt("qty");
        o.status             = rs.getString("status");
        o.cancellationReason = rs.getString("cancellation_reason");
        o.receivedAt         = rs.getString("received_at");
        o.updatedAt          = rs.getString("updated_at");
        return o;
    };
}
