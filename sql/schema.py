import sqlite3
import os


def schema():
    patients = """
        CREATE TABLE [IF NOT EXISTS] Patients(
        patient_id TEXT PRIMARY KEY,
        age INTEGER,
        gender TEXT DEFAULT "U",
        covid TEXT DEFAULT "U",
        died INTEGER DEFAULT 0,
        criticalCareStay INTEGER DEFAULT 0
        );
    """
    outcome = """
            CREATE TABLE [IF NOT EXISTS] Outcome(
            patient_id TEXT PRIMARY KEY,
            component TEXT,
            event_type TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_time REAL,
            covid_status TEXT DEFAULT "U",
            critical_care_admission INTEGER DEFAULT 0,
            source TEXT,
            source_type TEXT,
            destination TEXT,
            wimd INTEGER 
            );
        """
    measurements = """
        CREATE TABLE [IF NOT EXISTS] Measurements(
        patient_id TEXT PRIMARY KEY,
        result_name TEXT NOT NULL,
        result_date TEXT,
        result_time REAL,
        request_source TEXT,
        notes TEXT,
        flags TEXT
        );
    """
    critical_care = """
        CREATE TABLE [IF NOT EXISTS] CriticalCare(
        patient_id TEXT PRIMARY KEY,
        admission_date TEXT,
        admission_time REAL,
        discharge_date TEXT,
        discharge_time REAL,
        request_location TEXT,
        icu_days INTEGER,
        ventilated TEXT DEFAULT "U",
        covid_status TEXT DEFAULT "U"
        );
    """
    comorbidities = """
        CREATE TABLE [IF NOT EXISTS] Comorbidities(
        patient_id TEXT PRIMARY KEY,
        comorbs TEXT
        );
    """
    comorb_key = """CREATE TABLE [IF NOT EXISTS] ComorbKey(
    comorb_name TEXT PRIMARY KEY
    );"""
    return [patients, outcome, measurements, critical_care, comorbidities, comorb_key]


def create_database(db_path: str,
                    overwrite: bool = False,
                    **kwargs):
    if os.path.exists(db_path):
        if overwrite:
            os.remove(db_path)
        else:
            raise ValueError("Database already exists, set overwrite to True too drop database and generate "
                             "a new database, otherwise specify a new path")
    conn = sqlite3.connect(db_path, **kwargs)
    curr = conn.cursor()
    for x in schema():
        curr.execute(x)
    conn.commit()
    conn.close()

