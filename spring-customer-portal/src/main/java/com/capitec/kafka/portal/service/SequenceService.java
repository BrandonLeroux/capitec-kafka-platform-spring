package com.capitec.kafka.portal.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Service
public class SequenceService {

    private static final Logger log = LoggerFactory.getLogger(SequenceService.class);
    private static final long START = 1_000_000_000L;

    private final String dbPath;
    private final String orderServiceUrl;
    private Connection conn;

    public SequenceService(@Value("${app.db.path}") String dbPath,
                           @Value("${app.order-service.url}") String orderServiceUrl) {
        this.dbPath          = dbPath;
        this.orderServiceUrl = orderServiceUrl;
    }

    @PostConstruct
    public void init() throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath + "?busy_timeout=10000");
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS customer_sequence (last_number BIGINT NOT NULL)");
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM customer_sequence");
        if (rs.next() && rs.getInt(1) == 0) {
            long watermark = fetchWatermark();
            long seed = Math.max(watermark, START - 1);
            conn.createStatement().execute("INSERT INTO customer_sequence VALUES (" + seed + ")");
            log.info("Sequence initialised at {} (watermark={})", seed + 1, watermark);
        }
    }

    public synchronized long next() throws Exception {
        conn.createStatement().execute("UPDATE customer_sequence SET last_number = last_number + 1");
        ResultSet rs = conn.createStatement().executeQuery("SELECT last_number FROM customer_sequence");
        return rs.next() ? rs.getLong(1) : START;
    }

    private long fetchWatermark() {
        try {
            var rt = new RestTemplate();
            var resp = rt.getForObject(orderServiceUrl + "/api/customer/max-number", java.util.Map.class);
            if (resp != null && resp.get("maxCustomerNumber") instanceof Number n) return n.longValue();
        } catch (Exception e) {
            log.warn("Could not fetch watermark from order-service: {}", e.getMessage());
        }
        return START - 1;
    }
}
