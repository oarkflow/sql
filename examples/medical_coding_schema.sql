-- Master table for encounters
CREATE TABLE IF NOT EXISTS tbl_encounter_detail (
    encounter_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50),
    service_date DATE,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDI Tables
CREATE TABLE IF NOT EXISTS tbl_event_cdi (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    cdi_code VARCHAR(50),
    cdi_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_cdi_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    cdi_code VARCHAR(50),
    pro_info TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CPT Tables
CREATE TABLE IF NOT EXISTS tbl_event_cpt_fac (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    cpt_code VARCHAR(50),
    units INT,
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_cpt_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    cpt_code VARCHAR(50),
    units INT,
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- EM Tables
CREATE TABLE IF NOT EXISTS tbl_event_em_fac (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    em_code VARCHAR(50),
    level VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_em_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    em_code VARCHAR(50),
    level VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_em_pro_downcode (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    original_code VARCHAR(50),
    downcoded_code VARCHAR(50),
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- HCPCS Tables
CREATE TABLE IF NOT EXISTS tbl_event_hcpcs_fac (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    hcpcs_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_hcpcs_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    hcpcs_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ICD10 DX Tables
CREATE TABLE IF NOT EXISTS tbl_event_icd10_dx_fac (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    dx_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_icd10_dx_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    dx_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PQRI/PQRS Tables
CREATE TABLE IF NOT EXISTS tbl_event_pqri_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    pqri_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_pqrs_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    pqrs_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Special Tables
CREATE TABLE IF NOT EXISTS tbl_event_special_fac (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    special_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tbl_event_special_pro (
    id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(50),
    special_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
