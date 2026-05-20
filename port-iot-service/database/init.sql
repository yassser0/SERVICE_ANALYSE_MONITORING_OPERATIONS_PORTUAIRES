-- =============================================================================
-- PORT IoT DATA SERVICE — PostgreSQL Schema
-- Marsa Maroc Smart Port Platform
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =============================================================================
-- ENUM TYPES
-- =============================================================================
DO $$ BEGIN
    CREATE TYPE equipment_status_enum AS ENUM (
        'ACTIVE', 'IDLE', 'MAINTENANCE', 'OFFLINE', 'ERROR'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE sensor_type_enum AS ENUM (
        'GPS', 'TEMPERATURE', 'MOVEMENT', 'EQUIPMENT_STATUS', 'COMBINED'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE zone_enum AS ENUM (
        'QUAY_A', 'QUAY_B', 'QUAY_C', 'YARD_NORTH', 'YARD_SOUTH',
        'GATE_ENTRY', 'GATE_EXIT', 'WAREHOUSE_1', 'WAREHOUSE_2', 'MAINTENANCE_AREA'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE anomaly_type_enum AS ENUM (
        'HIGH_TEMPERATURE', 'INACTIVE_CONTAINER', 'INVALID_GPS',
        'EQUIPMENT_ERROR', 'SENSOR_OFFLINE', 'UNEXPECTED_MOVEMENT',
        'LOW_BATTERY', 'SPEED_VIOLATION'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE severity_enum AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE vessel_status_enum AS ENUM ('DOCKED', 'LOADING', 'UNLOADING', 'DEPARTING', 'ANCHORED');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- =============================================================================
-- TABLE: sensors
-- =============================================================================
CREATE TABLE IF NOT EXISTS sensors (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    sensor_id           VARCHAR(50) NOT NULL UNIQUE,
    sensor_type         sensor_type_enum NOT NULL DEFAULT 'COMBINED',
    zone                zone_enum,
    location_name       VARCHAR(100),
    manufacturer        VARCHAR(100),
    model               VARCHAR(100),
    firmware_version    VARCHAR(20),
    installation_date   DATE,
    last_seen           TIMESTAMPTZ,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    metadata            JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensors_sensor_id   ON sensors(sensor_id);
CREATE INDEX IF NOT EXISTS idx_sensors_zone        ON sensors(zone);
CREATE INDEX IF NOT EXISTS idx_sensors_is_active   ON sensors(is_active);

-- =============================================================================
-- TABLE: vessels
-- =============================================================================
CREATE TABLE IF NOT EXISTS vessels (
    vessel_id           VARCHAR(20)  PRIMARY KEY,
    vessel_name         VARCHAR(100) NOT NULL,
    flag_country        VARCHAR(50),
    vessel_type         VARCHAR(50)  DEFAULT 'Container Ship',
    imo_number          VARCHAR(20),
    berth_location      zone_enum,
    arrival_time        TIMESTAMPTZ,
    departure_time      TIMESTAMPTZ,
    status              vessel_status_enum NOT NULL DEFAULT 'DOCKED',
    captain_name        VARCHAR(100),
    gross_tonnage       NUMERIC(12,2),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vessels_status       ON vessels(status);
CREATE INDEX IF NOT EXISTS idx_vessels_berth        ON vessels(berth_location);

-- =============================================================================
-- TABLE: containers
-- =============================================================================
CREATE TABLE IF NOT EXISTS containers (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    container_id        VARCHAR(20) NOT NULL UNIQUE,
    owner               VARCHAR(100),
    container_type      VARCHAR(20) DEFAULT '20ft',
    max_weight_kg       NUMERIC(10,2),
    vessel_id           VARCHAR(20) REFERENCES vessels(vessel_id),
    current_zone        zone_enum,
    current_lat         NUMERIC(10,7),
    current_lon         NUMERIC(10,7),
    prev_lat            NUMERIC(10,7),
    prev_lon            NUMERIC(10,7),
    equipment_status    equipment_status_enum NOT NULL DEFAULT 'ACTIVE',
    temperature_celsius NUMERIC(5,2),
    last_movement       TIMESTAMPTZ,
    last_seen           TIMESTAMPTZ,
    is_flagged          BOOLEAN     NOT NULL DEFAULT FALSE,
    metadata            JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_containers_container_id      ON containers(container_id);
CREATE INDEX IF NOT EXISTS idx_containers_zone              ON containers(current_zone);
CREATE INDEX IF NOT EXISTS idx_containers_equipment_status  ON containers(equipment_status);
CREATE INDEX IF NOT EXISTS idx_containers_is_flagged        ON containers(is_flagged);
CREATE INDEX IF NOT EXISTS idx_containers_vessel_id         ON containers(vessel_id);

-- =============================================================================
-- TABLE: sensor_events  (time-series partitioned)
-- =============================================================================
CREATE TABLE IF NOT EXISTS sensor_events (
    id                  BIGSERIAL,
    event_id            UUID        NOT NULL DEFAULT uuid_generate_v4(),
    sensor_id           VARCHAR(50) NOT NULL,
    container_id        VARCHAR(20),
    vessel_id           VARCHAR(20),
    temperature_celsius NUMERIC(5,2),
    humidity            NUMERIC(5,2),
    zone                zone_enum,
    equipment_status    equipment_status_enum,
    latitude            NUMERIC(10,7),
    longitude           NUMERIC(10,7),
    speed_kmh           NUMERIC(7,2),
    heading_degrees     NUMERIC(6,2),
    battery_level       NUMERIC(5,2),
    signal_strength     NUMERIC(7,2),
    load_percentage     NUMERIC(5,2),
    raw_payload         JSONB       NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_timestamp     TIMESTAMPTZ NOT NULL,
    is_anomaly          BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS sensor_events_2026_05
    PARTITION OF sensor_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS sensor_events_2026_06
    PARTITION OF sensor_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE IF NOT EXISTS sensor_events_2026_07
    PARTITION OF sensor_events
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE IF NOT EXISTS sensor_events_2026_08
    PARTITION OF sensor_events
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE INDEX IF NOT EXISTS idx_events_sensor_id        ON sensor_events(sensor_id);
CREATE INDEX IF NOT EXISTS idx_events_container_id     ON sensor_events(container_id);
CREATE INDEX IF NOT EXISTS idx_events_event_timestamp  ON sensor_events(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_is_anomaly       ON sensor_events(is_anomaly) WHERE is_anomaly = TRUE;

-- =============================================================================
-- TABLE: anomalies
-- =============================================================================
CREATE TABLE IF NOT EXISTS anomalies (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id            UUID,
    sensor_id           VARCHAR(50) NOT NULL,
    container_id        VARCHAR(20),
    anomaly_type        anomaly_type_enum NOT NULL,
    severity            severity_enum NOT NULL DEFAULT 'MEDIUM',
    description         TEXT        NOT NULL,
    detected_value      NUMERIC(10,4),
    threshold_value     NUMERIC(10,4),
    zone                zone_enum,
    is_resolved         BOOLEAN     NOT NULL DEFAULT FALSE,
    resolved_at         TIMESTAMPTZ,
    resolved_by         VARCHAR(100),
    raw_payload         JSONB,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_sensor_id      ON anomalies(sensor_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_container_id   ON anomalies(container_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_anomaly_type   ON anomalies(anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity       ON anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_is_resolved    ON anomalies(is_resolved);
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at    ON anomalies(detected_at DESC);

-- =============================================================================
-- TABLE: statistics_cache (materialized stats for dashboards)
-- =============================================================================
CREATE TABLE IF NOT EXISTS statistics_cache (
    id                  SERIAL      PRIMARY KEY,
    stat_key            VARCHAR(100) NOT NULL UNIQUE,
    stat_value          JSONB       NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- FUNCTIONS & TRIGGERS
-- =============================================================================

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sensors_updated_at
    BEFORE UPDATE ON sensors
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_containers_updated_at
    BEFORE UPDATE ON containers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_vessels_updated_at
    BEFORE UPDATE ON vessels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- SEED DATA: Initial sensors
-- =============================================================================
INSERT INTO sensors (sensor_id, sensor_type, zone, location_name, manufacturer, model, firmware_version, installation_date, is_active)
VALUES
    ('SNS-GPS-001',  'GPS',              'QUAY_A',          'Quay A Berth 1',       'Siemens',  'GPS-Pro-X1',    '2.1.0', '2024-01-15', TRUE),
    ('SNS-GPS-002',  'GPS',              'QUAY_B',          'Quay B Berth 2',       'Siemens',  'GPS-Pro-X1',    '2.1.0', '2024-01-15', TRUE),
    ('SNS-GPS-003',  'GPS',              'QUAY_C',          'Quay C Berth 1',       'Siemens',  'GPS-Pro-X2',    '2.2.0', '2024-03-01', TRUE),
    ('SNS-GPS-004',  'GPS',              'YARD_NORTH',      'Yard North Entry',     'Siemens',  'GPS-Pro-X2',    '2.2.0', '2024-03-01', TRUE),
    ('SNS-TEMP-001', 'TEMPERATURE',      'WAREHOUSE_1',     'Cold Storage Block A', 'Honeywell','TempSens-T200', '1.5.3', '2024-02-01', TRUE),
    ('SNS-TEMP-002', 'TEMPERATURE',      'WAREHOUSE_2',     'Cold Storage Block B', 'Honeywell','TempSens-T200', '1.5.3', '2024-02-01', TRUE),
    ('SNS-TEMP-003', 'TEMPERATURE',      'QUAY_A',          'Quay A Reefer Stack',  'Honeywell','TempSens-T300', '1.6.0', '2024-04-10', TRUE),
    ('SNS-TEMP-004', 'TEMPERATURE',      'YARD_SOUTH',      'Yard South Cold Zone', 'Honeywell','TempSens-T300', '1.6.0', '2024-04-10', TRUE),
    ('SNS-MOV-001',  'MOVEMENT',         'YARD_NORTH',      'Yard North Gate',      'ABB',      'MovSens-M100',  '3.0.1', '2024-03-10', TRUE),
    ('SNS-MOV-002',  'MOVEMENT',         'YARD_SOUTH',      'Yard South Gate',      'ABB',      'MovSens-M100',  '3.0.1', '2024-03-10', TRUE),
    ('SNS-MOV-003',  'MOVEMENT',         'GATE_ENTRY',      'Main Gate Entry',      'ABB',      'MovSens-M200',  '3.1.0', '2024-05-01', TRUE),
    ('SNS-MOV-004',  'MOVEMENT',         'GATE_EXIT',       'Main Gate Exit',       'ABB',      'MovSens-M200',  '3.1.0', '2024-05-01', TRUE),
    ('SNS-EQP-001',  'EQUIPMENT_STATUS', 'GATE_ENTRY',      'Entry Gate Crane 1',   'Liebherr', 'EqpMon-E300',   '4.2.0', '2024-01-20', TRUE),
    ('SNS-EQP-002',  'EQUIPMENT_STATUS', 'MAINTENANCE_AREA','Maintenance Bay 1',    'Liebherr', 'EqpMon-E300',   '4.2.0', '2024-01-20', TRUE),
    ('SNS-EQP-003',  'EQUIPMENT_STATUS', 'QUAY_A',          'Quay A STS Crane',     'Liebherr', 'EqpMon-E400',   '4.3.0', '2024-06-01', TRUE),
    ('SNS-EQP-004',  'EQUIPMENT_STATUS', 'QUAY_B',          'Quay B RTG Crane',     'Liebherr', 'EqpMon-E400',   '4.3.0', '2024-06-01', TRUE),
    ('SNS-CMB-001',  'COMBINED',         'QUAY_C',          'Quay C Multi Sensor',  'Schneider','SmartNode-S500','5.0.0', '2024-04-05', TRUE),
    ('SNS-CMB-002',  'COMBINED',         'GATE_EXIT',       'Exit Gate Multi Sensor','Schneider','SmartNode-S500','5.0.0', '2024-04-05', TRUE),
    ('SNS-CMB-003',  'COMBINED',         'WAREHOUSE_1',     'WH1 Smart Node',       'Schneider','SmartNode-S600','5.1.0', '2024-07-01', TRUE),
    ('SNS-CMB-004',  'COMBINED',         'YARD_NORTH',      'Yard North Smart Node','Schneider','SmartNode-S600','5.1.0', '2024-07-01', TRUE)
ON CONFLICT (sensor_id) DO NOTHING;

-- =============================================================================
-- SEED DATA: Vessels currently docked
-- =============================================================================
INSERT INTO vessels (vessel_id, vessel_name, flag_country, vessel_type, imo_number, berth_location, arrival_time, departure_time, status, captain_name, gross_tonnage)
VALUES
    ('MSC-AURORA',   'MSC Aurora',          'Panama',        'Container Ship', 'IMO9876543', 'QUAY_A', NOW() - INTERVAL '2 days',  NOW() + INTERVAL '1 day',  'UNLOADING', 'Capt. Ahmed Benali',   95000.00),
    ('CMA-LIBERTY',  'CMA CGM Liberty',     'France',        'Container Ship', 'IMO8765432', 'QUAY_B', NOW() - INTERVAL '1 day',   NOW() + INTERVAL '2 days', 'LOADING',   'Capt. Marie Dupont',   112000.00),
    ('EVER-BRIGHT',  'Evergreen Bright',    'Taiwan',        'Container Ship', 'IMO7654321', 'QUAY_C', NOW() - INTERVAL '6 hours', NOW() + INTERVAL '3 days', 'DOCKED',    'Capt. Chen Wei',        78000.00),
    ('MAERSK-STAR',  'Maersk Star',         'Denmark',       'Container Ship', 'IMO6543210', 'YARD_NORTH', NOW() - INTERVAL '3 days', NOW() + INTERVAL '12 hours', 'DEPARTING', 'Capt. Lars Hansen',  135000.00),
    ('COS-PACIFIC',  'COSCO Pacific',       'China',         'Bulk Carrier',   'IMO5432109', 'YARD_SOUTH', NOW() - INTERVAL '4 days', NOW() + INTERVAL '2 days', 'ANCHORED',  'Capt. Li Ming',        88000.00)
ON CONFLICT (vessel_id) DO NOTHING;

-- =============================================================================
-- VIEWS for Grafana / Power BI
-- =============================================================================
CREATE OR REPLACE VIEW v_live_sensor_stats AS
SELECT
    s.zone,
    COUNT(DISTINCT se.sensor_id)    AS active_sensors,
    COUNT(se.id)                    AS total_events_1h,
    AVG(se.temperature_celsius)     AS avg_temperature,
    MAX(se.temperature_celsius)     AS max_temperature,
    AVG(se.battery_level)           AS avg_battery_level,
    SUM(CASE WHEN se.is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
FROM sensors s
LEFT JOIN sensor_events se
    ON se.sensor_id = s.sensor_id
    AND se.event_timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY s.zone;

CREATE OR REPLACE VIEW v_container_positions AS
SELECT
    container_id,
    vessel_id,
    current_zone    AS zone,
    current_lat     AS latitude,
    current_lon     AS longitude,
    equipment_status,
    temperature_celsius,
    is_flagged,
    last_seen
FROM containers
WHERE last_seen >= NOW() - INTERVAL '15 minutes';

CREATE OR REPLACE VIEW v_anomaly_summary AS
SELECT
    anomaly_type,
    severity,
    zone,
    COUNT(*)                                    AS total_count,
    COUNT(*) FILTER (WHERE NOT is_resolved)     AS open_count,
    MAX(detected_at)                            AS latest_occurrence
FROM anomalies
WHERE detected_at >= NOW() - INTERVAL '24 hours'
GROUP BY anomaly_type, severity, zone
ORDER BY total_count DESC;

CREATE OR REPLACE VIEW v_vessel_status AS
SELECT
    v.vessel_id,
    v.vessel_name,
    v.flag_country,
    v.vessel_type,
    v.berth_location,
    v.status,
    v.arrival_time,
    v.departure_time,
    v.captain_name,
    v.gross_tonnage,
    COUNT(c.container_id) AS container_count,
    SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) AS flagged_containers
FROM vessels v
LEFT JOIN containers c ON c.vessel_id = v.vessel_id
GROUP BY v.vessel_id, v.vessel_name, v.flag_country, v.vessel_type,
         v.berth_location, v.status, v.arrival_time, v.departure_time,
         v.captain_name, v.gross_tonnage;

CREATE OR REPLACE VIEW v_sensor_health AS
SELECT
    s.sensor_id,
    s.sensor_type,
    s.zone,
    s.manufacturer,
    s.model,
    AVG(se.battery_level)                                           AS avg_battery,
    MIN(se.battery_level)                                           AS min_battery,
    AVG(se.signal_strength)                                         AS avg_signal,
    MAX(se.event_timestamp)                                         AS last_heartbeat,
    EXTRACT(EPOCH FROM (NOW() - MAX(se.event_timestamp)))/60        AS minutes_since_last,
    COUNT(se.id)                                                    AS events_last_hour,
    s.is_active
FROM sensors s
LEFT JOIN sensor_events se
    ON se.sensor_id = s.sensor_id
    AND se.event_timestamp > NOW() - INTERVAL '1 hour'
GROUP BY s.sensor_id, s.sensor_type, s.zone, s.manufacturer, s.model, s.is_active;
