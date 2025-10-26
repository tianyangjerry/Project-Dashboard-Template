import os
import datetime as dt
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from pymongo import MongoClient

load_dotenv()

# Postgres schema helper
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")  # CHANGE: "public" to your own schema name


def qualify(sql: str) -> str:
    # Replace occurrences of {S}.<table> with <schema>.<table>
    return sql.replace("{S}.", f"{PG_SCHEMA}.")


# CONFIG: Postgres and Mongo Queries
CONFIG = {
    "postgres": {
        "enabled": True,
        "uri": os.getenv("PG_URI", "postgresql+psycopg2://postgres:password@localhost:5432/postgres"),
        # Will read from your .env file
        "queries": {
            # CHANGE: Replace all the following Postgres queries with your own queries, for each user you identified for your project's Information System
            # Each query must have a unique name, an SQL string, a chart specification, tags (for user roles), and optional params (parameters)
            # :doctor_id, :nurse_id, :patient_name, etc., are placeholders. Their values will come from the dashboard sidebar.
            # User 1: DOCTORS
            "Doctor: patients under my care (table)": {
                "sql": """
                       SELECT p.patient_id, p.name AS patient, p.age, p.room_no
                       FROM {S}.patients p
                       WHERE p.doctor_id = :doctor_id
                       ORDER BY p.name;
                       """,
                "chart": {"type": "table"},
                "tags": ["doctor"],
                "params": ["doctor_id"]
            },
            "Doctor: most recent treatment per my patient (table)": {
                "sql": """
                       SELECT p.name AS patient,
                              (SELECT MAX(t.treatment_time)
                               FROM {S}.treatments t
                       WHERE t.patient_id = p.patient_id) AS last_treatment
                       FROM {S}.patients p
                       WHERE p.doctor_id = :doctor_id
                       ORDER BY last_treatment DESC NULLS LAST;
                       """,
                "chart": {"type": "table"},
                "tags": ["doctor"],
                "params": ["doctor_id"]
            },
            "Doctor: high-risk (age > threshold) under my care (bar)": {
                "sql": """
                       SELECT p.name AS patient, p.age
                       FROM {S}.patients p
                       WHERE p.doctor_id = :doctor_id
                         AND p.age
                           > :age_threshold
                       ORDER BY p.age DESC;
                       """,
                "chart": {"type": "bar", "x": "patient", "y": "age"},
                "tags": ["doctor"],
                "params": ["doctor_id", "age_threshold"]
            },
            "Doctor: patients with NO treatment today (table)": {
                "sql": """
                       SELECT p.name, p.room_no
                       FROM {S}.patients p
                       WHERE p.doctor_id = :doctor_id
                         AND NOT EXISTS (
                           SELECT 1
                           FROM {S}.treatments t
                           WHERE t.patient_id = p.patient_id
                         AND t.treatment_time:: date = CURRENT_DATE
                           );
                       """,
                "chart": {"type": "table"},
                "tags": ["doctor"],
                "params": ["doctor_id"]
            },
            "Doctor: treatments by type for my patients (bar)": {
                "sql": """
                       SELECT t.treatment_type, COUNT(*) ::int AS times_given
                       FROM {S}.treatments t
                    JOIN {S}.patients p
                       ON p.patient_id = t.patient_id
                       WHERE p.doctor_id = :doctor_id
                       GROUP BY t.treatment_type
                       ORDER BY times_given DESC;
                       """,
                "chart": {"type": "bar", "x": "treatment_type", "y": "times_given"},
                "tags": ["doctor"],
                "params": ["doctor_id"]
            },

            # User 2: NURSES
            "Nurse: today’s tasks (treatments to administer) (table)": {
                "sql": """
                       SELECT p.name AS patient, t.treatment_type, t.treatment_time
                       FROM {S}.treatments t
                    JOIN {S}.patients p
                       ON t.patient_id = p.patient_id
                       WHERE t.nurse_id = :nurse_id
                         AND t.treatment_time:: date = CURRENT_DATE
                       ORDER BY t.treatment_time;
                       """,
                "chart": {"type": "table"},
                "tags": ["nurse"],
                "params": ["nurse_id"]
            },
            "Nurse: patients with NO treatment yet today (table)": {
                "sql": """
                       SELECT p.name, p.room_no
                       FROM {S}.patients p
                       WHERE NOT EXISTS (
                           SELECT 1
                           FROM {S}.treatments t
                           WHERE t.patient_id = p.patient_id
                         AND t.treatment_time:: date = CURRENT_DATE
                           )
                       ORDER BY p.room_no, p.name;
                       """,
                "chart": {"type": "table"},
                "tags": ["nurse"]
            },
            "Nurse: medicines running low (bar)": {
                "sql": """
                       SELECT m.name, m.quantity
                       FROM {S}.medicine_stock m
                       WHERE m.quantity < :med_low_threshold
                       ORDER BY m.quantity ASC;
                       """,
                "chart": {"type": "bar", "x": "name", "y": "quantity"},
                "tags": ["nurse"],
                "params": ["med_low_threshold"]
            },

            # User 3: PHARMACISTS
            "Pharmacist: medicines to reorder (bar)": {
                "sql": """
                       SELECT m.name, m.quantity
                       FROM {S}.medicine_stock m
                       WHERE m.quantity < :reorder_threshold
                       ORDER BY m.quantity ASC;
                       """,
                "chart": {"type": "bar", "x": "name", "y": "quantity"},
                "tags": ["pharmacist"],
                "params": ["reorder_threshold"]
            },
            "Pharmacist: top 5 medicines this month (bar)": {
                "sql": """
                       SELECT t.treatment_type AS medicine, COUNT(*) ::int AS times_given
                       FROM {S}.treatments t
                       WHERE t.treatment_time >= date_trunc('month', CURRENT_DATE)
                       GROUP BY t.treatment_type
                       ORDER BY times_given DESC
                           LIMIT 5;
                       """,
                "chart": {"type": "bar", "x": "medicine", "y": "times_given"},
                "tags": ["pharmacist"]
            },
            "Pharmacist: which nurse gave most medicines today (table)": {
                "sql": """
                       SELECT n.name, COUNT(t.treatment_id) ::int AS total
                       FROM {S}.nurses n
                    JOIN {S}.treatments t
                       ON t.nurse_id = n.nurse_id
                       WHERE t.treatment_time:: date = CURRENT_DATE
                       GROUP BY n.name
                       ORDER BY total DESC
                           LIMIT 1;
                       """,
                "chart": {"type": "table"},
                "tags": ["pharmacist"]
            },
            "Pharmacist: medicines unused in last N days (table)": {
                "sql": """
                       SELECT m.name
                       FROM {S}.medicine_stock m
                       WHERE NOT EXISTS (
                           SELECT 1
                           FROM {S}.treatments t
                           WHERE t.treatment_type = m.name
                         AND t.treatment_time >= NOW() - (:days || ' days'):: interval
                           )
                       ORDER BY m.name;
                       """,
                "chart": {"type": "table"},
                "tags": ["pharmacist"],
                "params": ["days"]
            },

            # User 4: FAMILY/GUARDIANS 
            "Family: last treatment for my relative (table)": {
                "sql": """
                       SELECT t.treatment_type, t.treatment_time, n.name AS nurse
                       FROM {S}.treatments t
                    JOIN {S}.patients p
                       ON t.patient_id = p.patient_id
                           LEFT JOIN {S}.nurses n ON t.nurse_id = n.nurse_id
                       WHERE p.name = :patient_name
                       ORDER BY t.treatment_time DESC
                           LIMIT 1;
                       """,
                "chart": {"type": "table"},
                "tags": ["guardian"],
                "params": ["patient_name"]
            },
            "Family: which doctor is assigned to my relative? (table)": {
                "sql": """
                       SELECT p.name AS patient, d.name AS doctor, d.specialization
                       FROM {S}.patients p
                    JOIN {S}.doctors d
                       ON p.doctor_id = d.doctor_id
                       WHERE p.name = :patient_name;
                       """,
                "chart": {"type": "table"},
                "tags": ["guardian"],
                "params": ["patient_name"]
            },
            "Family: total treatments this month for my relative (table)": {
                "sql": """
                       SELECT COUNT(*) ::int AS treatments_this_month
                       FROM {S}.treatments t
                    JOIN {S}.patients p
                       ON t.patient_id = p.patient_id
                       WHERE p.name = :patient_name
                         AND t.treatment_time >= date_trunc('month'
                           , CURRENT_DATE);
                       """,
                "chart": {"type": "table"},
                "tags": ["guardian"],
                "params": ["patient_name"]
            },

            # User 5: MANAGERS 
            "Mgr: total patients & average age (table)": {
                "sql": """
                       SELECT COUNT(*)::int AS total_patients, AVG(age) ::numeric(10,1) AS avg_age
                       FROM {S}.patients;
                       """,
                "chart": {"type": "table"},
                "tags": ["manager"]
            },
            "Mgr: patients per doctor (bar)": {
                "sql": """
                       SELECT d.name AS doctor, COUNT(*) ::int AS num_patients
                       FROM {S}.doctors d
                    LEFT JOIN {S}.patients p
                       ON d.doctor_id = p.doctor_id
                       GROUP BY d.name
                       ORDER BY num_patients DESC;
                       """,
                "chart": {"type": "bar", "x": "doctor", "y": "num_patients"},
                "tags": ["manager"]
            },
            "Mgr: treatments in last N days (table)": {
                "sql": """
                       SELECT COUNT(*) ::int AS total_treatments
                       FROM {S}.treatments
                       WHERE treatment_time >= NOW() - (:days || ' days'):: interval;
                       """,
                "chart": {"type": "table"},
                "tags": ["manager"],
                "params": ["days"]
            },
            "Mgr: rooms currently occupied (table)": {
                "sql": """
                       SELECT DISTINCT p.room_no
                       FROM {S}.patients p
                       ORDER BY p.room_no;
                       """,
                "chart": {"type": "table"},
                "tags": ["manager"]
            },
            "Mgr: doctor with oldest patients (table)": {
                "sql": """
                       SELECT d.name, MAX(p.age) AS oldest_patient_age
                       FROM {S}.doctors d
                    JOIN {S}.patients p
                       ON d.doctor_id = p.doctor_id
                       GROUP BY d.name
                       ORDER BY oldest_patient_age DESC
                           LIMIT 1;
                       """,
                "chart": {"type": "table"},
                "tags": ["manager"]
            }
        }
    },

    "mongo": {
        "enabled": True,
        "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),  # Will read from the .env file
        "db_name": os.getenv("MONGO_DB", "eldercare"),  # Will read from the .env file

        # CHANGE: Just like above, replace all the following Mongo queries with your own, for the different users you identified
        "queries": {
            "TS: Hourly avg heart rate (resident 501, last 24h)": {
                "collection": "bracelet_readings_ts",
                "aggregate": [
                    {"$match": {
                        "meta.resident_id": 501,
                        "ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}
                    }},
                    {"$project": {
                        "hour": {"$dateTrunc": {"date": "$ts", "unit": "hour"}},
                        "hr": "$heart_rate_bpm"
                    }},
                    {"$group": {"_id": "$hour", "avg_hr": {"$avg": "$hr"}, "n": {"$count": {}}}},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "line", "x": "_id", "y": "avg_hr"}
            },

            "TS: Exceedance counts (SpO2 < 92, last 7 days) by resident": {
                "collection": "bracelet_readings_ts",
                "aggregate": [
                    {"$match": {
                        "ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)},
                        "spo2_pct": {"$lt": 92}
                    }},
                    {"$group": {"_id": "$meta.resident_id", "hits": {"$count": {}}}},
                    {"$sort": {"hits": -1}}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "hits"}
            },

            "Telemetry: Latest reading per device": {
                "collection": "bracelet_data",
                "aggregate": [
                    {"$sort": {"ts": -1, "_id": -1}},
                    {"$group": {"_id": "$device_id", "doc": {"$first": "$$ROOT"}}},
                    {"$replaceRoot": {"newRoot": "$doc"}},
                    {"$project": {
                        "_id": 0, "device_id": 1, "resident_id": 1, "ts": 1,
                        "hr": "$metrics.heart_rate_bpm", "spo2": "$metrics.spo2_pct",
                        "status": 1
                    }}
                ],
                "chart": {"type": "table"}
            },

            "Telemetry: Battery status distribution": {
                "collection": "bracelet_data",
                "aggregate": [
                    {"$project": {
                        "battery": {"$ifNull": ["$battery_pct", None]},
                        "bucket": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$gte": ["$battery_pct", 80]}, "then": "80–100"},
                                    {"case": {"$gte": ["$battery_pct", 60]}, "then": "60–79"},
                                    {"case": {"$gte": ["$battery_pct", 40]}, "then": "40–59"},
                                    {"case": {"$gte": ["$battery_pct", 20]}, "then": "20–39"},
                                ],
                                "default": "<20 or null"
                            }
                        }
                    }},
                    {"$group": {"_id": "$bucket", "cnt": {"$count": {}}}},
                    {"$sort": {"cnt": -1}}
                ],
                "chart": {"type": "pie", "names": "_id", "values": "cnt"}
            },

            "TS Treemap: readings count by resident and device (last 24h)": {
                "collection": "bracelet_readings_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}}},
                    {"$group": {"_id": {"resident": "$meta.resident_id", "device": "$meta.device_id"},
                                "cnt": {"$count": {}}}},
                    {"$project": {"resident": "$_id.resident", "device": "$_id.device", "cnt": 1, "_id": 0}}
                ],
                "chart": {"type": "treemap", "path": ["resident", "device"], "values": "cnt"}
            }
        }
    }
}

# The following block of code will create a simple Streamlit dashboard page
st.set_page_config(page_title="Old-Age Home DB Dashboard", layout="wide")
st.title("Old-Age Home | Mini Dashboard (Postgres + MongoDB)")


def metric_row(metrics: dict):
    cols = st.columns(len(metrics))
    for (k, v), c in zip(metrics.items(), cols):
        c.metric(k, v)


@st.cache_resource
def get_pg_engine(uri: str):
    return create_engine(uri, pool_pre_ping=True, future=True)


@st.cache_data(ttl=60)
def run_pg_query(_engine, sql: str, params: dict | None = None):
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


@st.cache_resource
def get_mongo_client(uri: str):
    return MongoClient(uri)


def mongo_overview(client: MongoClient, db_name: str):
    info = client.server_info()
    db = client[db_name]
    colls = db.list_collection_names()
    stats = db.command("dbstats")
    total_docs = sum(db[c].estimated_document_count() for c in colls) if colls else 0
    return {
        "DB": db_name,
        "Collections": f"{len(colls):,}",
        "Total docs (est.)": f"{total_docs:,}",
        "Storage": f"{round(stats.get('storageSize', 0) / 1024 / 1024, 1)} MB",
        "Version": info.get("version", "unknown")
    }


@st.cache_data(ttl=60)
def run_mongo_aggregate(_client, db_name: str, coll: str, stages: list):
    db = _client[db_name]
    docs = list(db[coll].aggregate(stages, allowDiskUse=True))
    return pd.json_normalize(docs) if docs else pd.DataFrame()


def render_chart(df: pd.DataFrame, spec: dict):
    if df.empty:
        st.info("No rows.")
        return
    ctype = spec.get("type", "table")
    # light datetime parsing for x axes
    for c in df.columns:
        if df[c].dtype == "object":
            try:
                df[c] = pd.to_datetime(df[c])
            except Exception:
                pass

    if ctype == "table":
        st.dataframe(df, use_container_width=True)
    elif ctype == "line":
        st.plotly_chart(px.line(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "bar":
        st.plotly_chart(px.bar(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "pie":
        st.plotly_chart(px.pie(df, names=spec["names"], values=spec["values"]), use_container_width=True)
    elif ctype == "heatmap":
        pivot = pd.pivot_table(df, index=spec["rows"], columns=spec["cols"], values=spec["values"], aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, aspect="auto", origin="upper",
                                  labels=dict(x=spec["cols"], y=spec["rows"], color=spec["values"])),
                        use_container_width=True)
    elif ctype == "treemap":
        st.plotly_chart(px.treemap(df, path=spec["path"], values=spec["values"]), use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)


# The following block of code is for the dashboard sidebar, where you can pick your users, provide parameters, etc.
with st.sidebar:
    st.header("Connections")
    # These fields are pre-filled from .env file
    pg_uri = st.text_input("Postgres URI", CONFIG["postgres"]["uri"])
    mongo_uri = st.text_input("Mongo URI", CONFIG["mongo"]["uri"])
    mongo_db = st.text_input("Mongo DB name", CONFIG["mongo"]["db_name"])
    st.divider()
    auto_run = st.checkbox("Auto-run on selection change", value=False, key="auto_run_global")

    st.header("Role & Parameters")
    # CHANGE: Change the different roles, the specific attributes, parameters used, etc., to match your own Information System
    role = st.selectbox("User role", ["doctor", "nurse", "pharmacist", "guardian", "manager", "all"], index=5)
    doctor_id = st.number_input("doctor_id", min_value=1, value=1, step=1)
    nurse_id = st.number_input("nurse_id", min_value=1, value=2, step=1)
    patient_name = st.text_input("patient_name", value="Alice")
    age_threshold = st.number_input("age_threshold", min_value=0, value=85, step=1)
    days = st.slider("last N days", 1, 90, 7)
    med_low_threshold = st.number_input("med_low_threshold", min_value=0, value=5, step=1)
    reorder_threshold = st.number_input("reorder_threshold", min_value=0, value=10, step=1)

    PARAMS_CTX = {
        "doctor_id": int(doctor_id),
        "nurse_id": int(nurse_id),
        "patient_name": patient_name,
        "age_threshold": int(age_threshold),
        "days": int(days),
        "med_low_threshold": int(med_low_threshold),
        "reorder_threshold": int(reorder_threshold),
    }

# Postgres part of the dashboard
st.subheader("Postgres")
try:

    eng = get_pg_engine(pg_uri)

    with st.expander("Run Postgres query", expanded=True):
        # The following will filter queries by role
        def filter_queries_by_role(qdict: dict, role: str) -> dict:
            def ok(tags):
                t = [s.lower() for s in (tags or ["all"])]
                return "all" in t or role.lower() in t

            return {name: q for name, q in qdict.items() if ok(q.get("tags"))}


        pg_all = CONFIG["postgres"]["queries"]
        pg_q = filter_queries_by_role(pg_all, role)

        names = list(pg_q.keys()) or ["(no queries for this role)"]
        sel = st.selectbox("Choose a saved query", names, key="pg_sel")

        if sel in pg_q:
            q = pg_q[sel]
            sql = qualify(q["sql"])
            st.code(sql, language="sql")

            run = auto_run or st.button("▶ Run Postgres", key="pg_run")
            if run:
                wanted = q.get("params", [])
                params = {k: PARAMS_CTX[k] for k in wanted}
                df = run_pg_query(eng, sql, params=params)
                render_chart(df, q["chart"])
        else:
            st.info("No Postgres queries tagged for this role.")
except Exception as e:
    st.error(f"Postgres error: {e}")

# Mongo panel
if CONFIG["mongo"]["enabled"]:
    st.subheader("🍃 MongoDB")
    try:
        mongo_client = get_mongo_client(mongo_uri)
        metric_row(mongo_overview(mongo_client, mongo_db))

        with st.expander("Run Mongo aggregation", expanded=True):
            mongo_query_names = list(CONFIG["mongo"]["queries"].keys())
            selm = st.selectbox("Choose a saved aggregation", mongo_query_names, key="mongo_sel")
            q = CONFIG["mongo"]["queries"][selm]
            st.write(f"**Collection:** `{q['collection']}`")
            st.code(str(q["aggregate"]), language="python")
            runm = auto_run or st.button("▶ Run Mongo", key="mongo_run")
            if runm:
                dfm = run_mongo_aggregate(mongo_client, mongo_db, q["collection"], q["aggregate"])
                render_chart(dfm, q["chart"])
    except Exception as e:
        st.error(f"Mongo error: {e}")