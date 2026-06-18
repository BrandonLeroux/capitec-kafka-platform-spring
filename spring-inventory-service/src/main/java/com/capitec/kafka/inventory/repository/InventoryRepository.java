package com.capitec.kafka.inventory.repository;

import com.capitec.kafka.inventory.model.InventoryItem;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Repository
public class InventoryRepository {

    private final JdbcTemplate jdbc;

    public InventoryRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @PostConstruct
    public void init() {
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                sku           TEXT PRIMARY KEY,
                product_id    TEXT NOT NULL,
                name          TEXT,
                category      TEXT,
                quantity      INTEGER DEFAULT 0,
                reorder_level INTEGER DEFAULT 10,
                unit_price    REAL DEFAULT 0,
                updated_at    TEXT DEFAULT (datetime('now'))
            )""");
        jdbc.execute("PRAGMA journal_mode=WAL");
        jdbc.execute("PRAGMA busy_timeout=10000");
    }

    public synchronized void upsert(InventoryItem i) {
        jdbc.update("""
            INSERT INTO inventory (sku,product_id,name,category,quantity,reorder_level,unit_price,updated_at)
            VALUES (?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(sku) DO UPDATE SET
                quantity=excluded.quantity, name=COALESCE(excluded.name,name),
                category=COALESCE(excluded.category,category),
                reorder_level=COALESCE(excluded.reorder_level,reorder_level),
                unit_price=COALESCE(excluded.unit_price,unit_price),
                updated_at=datetime('now')
            """, i.sku, i.productID, i.name, i.category, i.quantity, i.reorderLevel, i.unitPrice);
    }

    public synchronized void adjust(String sku, int delta) {
        jdbc.update("UPDATE inventory SET quantity=MAX(0,quantity+?), updated_at=datetime('now') WHERE sku=?", delta, sku);
    }

    public InventoryItem findBySku(String sku) {
        var list = jdbc.query("SELECT * FROM inventory WHERE sku=?", ROW_MAPPER, sku);
        return list.isEmpty() ? null : list.get(0);
    }

    public InventoryItem findByProductId(String pid) {
        var list = jdbc.query("SELECT * FROM inventory WHERE product_id=?", ROW_MAPPER, pid);
        return list.isEmpty() ? null : list.get(0);
    }

    public List<InventoryItem> findAll(String search, String category, int limit, int offset) {
        var sql = new StringBuilder("SELECT * FROM inventory WHERE 1=1");
        var args = new java.util.ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (sku LIKE ? OR name LIKE ? OR product_id LIKE ?)");
            String like = "%" + search + "%"; args.add(like); args.add(like); args.add(like);
        }
        if (category != null && !category.isBlank() && !"ALL".equals(category)) { sql.append(" AND category=?"); args.add(category); }
        sql.append(" ORDER BY category,name LIMIT ? OFFSET ?");
        args.add(limit); args.add(offset);
        return jdbc.query(sql.toString(), ROW_MAPPER, args.toArray());
    }

    public int count(String search, String category) {
        var sql = new StringBuilder("SELECT COUNT(*) FROM inventory WHERE 1=1");
        var args = new java.util.ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (sku LIKE ? OR name LIKE ? OR product_id LIKE ?)");
            String like = "%" + search + "%"; args.add(like); args.add(like); args.add(like);
        }
        if (category != null && !category.isBlank() && !"ALL".equals(category)) { sql.append(" AND category=?"); args.add(category); }
        return jdbc.queryForObject(sql.toString(), Integer.class, args.toArray());
    }

    public long countLowStock() {
        return jdbc.queryForObject("SELECT COUNT(*) FROM inventory WHERE quantity<=reorder_level", Long.class);
    }

    public Map<String, Object> stockMap() {
        var result = new java.util.LinkedHashMap<String, Object>();
        jdbc.query("SELECT product_id, sku, quantity FROM inventory", rs -> {
            result.put(rs.getString("product_id"), Map.of("qty", rs.getInt("quantity"), "sku", rs.getString("sku")));
        });
        return result;
    }

    private static final RowMapper<InventoryItem> ROW_MAPPER = (rs, i) -> {
        var item = new InventoryItem();
        item.sku          = rs.getString("sku");
        item.productID    = rs.getString("product_id");
        item.name         = rs.getString("name");
        item.category     = rs.getString("category");
        item.quantity     = rs.getInt("quantity");
        item.reorderLevel = rs.getInt("reorder_level");
        item.unitPrice    = rs.getDouble("unit_price");
        item.updatedAt    = rs.getString("updated_at");
        return item;
    };
}
