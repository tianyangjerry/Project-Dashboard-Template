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
        "uri": os.getenv("PG_URI", "postgresql+psycopg2://postgres:postgres@localhost:5432/streaming_db"),
        "queries": {
            # ===== Viewer =====
            "Viewer: my watchlist (table)": {
                "sql": """
                       SELECT w.added_at, c.content_id, c.title, c.content_type
                       FROM {S}.watchlist w
            JOIN {S}.content   c
                       ON c.content_id = w.content_id
                       WHERE w.profile_id = :profile_id
                       ORDER BY w.added_at DESC
                           LIMIT 200;
                       """,
                "chart": {"type": "table"},
                "tags": ["viewer"],
                "params": ["profile_id"]
            },
            "Viewer: my ratings in last N days (table)": {
                "sql": """
                       SELECT r.created_at, c.title, r.stars, r.comment
                       FROM {S}.ratings r
            JOIN {S}.content c
                       ON c.content_id = r.content_id
                       WHERE r.profile_id = :profile_id
                         AND r.created_at >= NOW() - (:since_days || ' days'):: interval
                       ORDER BY r.created_at DESC
                           LIMIT 200;
                       """,
                "chart": {"type": "table"},
                "tags": ["viewer"],
                "params": ["profile_id", "since_days"]
            },
            "Viewer: rating histogram last N days (bar)": {
                "sql": """
                       SELECT r.stars, COUNT(*) ::int AS cnt
                       FROM {S}.ratings r
                       WHERE r.profile_id = :profile_id
                         AND r.created_at >= NOW() - (:since_days || ' days'):: interval
                       GROUP BY r.stars
                       ORDER BY r.stars;
                       """,
                "chart": {"type": "bar", "x": "stars", "y": "cnt"},
                "tags": ["viewer"],
                "params": ["profile_id", "since_days"]
            },
            "Viewer: my avg stars by genre (bar)": {
                "sql": """
                       SELECT g.name                          AS genre,
                              ROUND(AVG(r.stars)::numeric, 2) AS avg_stars,
                              COUNT(*) ::int AS n
                       FROM {S}.ratings r
            JOIN {S}.content_genres cg
                       ON cg.content_id = r.content_id
                           JOIN {S}.genres g ON g.genre_id = cg.genre_id
                       WHERE r.profile_id = :profile_id
                       GROUP BY g.name
                       HAVING COUNT (*) >= 2
                       ORDER BY avg_stars DESC
                           LIMIT 20;
                       """,
                "chart": {"type": "bar", "x": "genre", "y": "avg_stars"},
                "tags": ["viewer"],
                "params": ["profile_id"]
            },

            # ===== Support =====
            "Support: user overview (table)": {
                "sql": """
                       SELECT u.user_id,
                              u.email,
                              u.country_code,
                              u.is_active,
                              u.created_at,
                              COALESCE(sub.status, 'none') AS active_sub_status,
                              COALESCE(pl.name, '')        AS active_plan,
                              COALESCE(pz.pcnt, 0)         AS profiles,
                              COALESCE(dz.dcnt, 0)         AS devices
                       FROM {S}.users u
            LEFT JOIN LATERAL (
                SELECT s.status, s.plan_id
                FROM {S}.subscriptions s
                WHERE s.user_id = u.user_id AND s.status = 'active'
                ORDER BY s.start_at DESC
                LIMIT 1
            ) sub
                       ON TRUE
                           LEFT JOIN {S}.plans pl ON pl.plan_id = sub.plan_id
                           LEFT JOIN LATERAL (
                           SELECT COUNT (*):: int AS pcnt FROM {S}.profiles p WHERE p.user_id = u.user_id
                           ) pz ON TRUE
                           LEFT JOIN LATERAL (
                           SELECT COUNT (*):: int AS dcnt FROM {S}.devices d WHERE d.user_id = u.user_id
                           ) dz ON TRUE
                       WHERE u.user_id = :user_id;
                       """,
                "chart": {"type": "table"},
                "tags": ["support"],
                "params": ["user_id"]
            },
            "Support: profiles by user (table)": {
                "sql": """
                       SELECT profile_id, name, maturity_rating, preferred_lang, created_at
                       FROM {S}.profiles
                       WHERE user_id = :user_id
                       ORDER BY created_at DESC;
                       """,
                "chart": {"type": "table"},
                "tags": ["support"],
                "params": ["user_id"]
            },
            "Support: devices by user (table)": {
                "sql": """
                       SELECT device_id, device_type, app_version, last_seen_at
                       FROM {S}.devices
                       WHERE user_id = :user_id
                       ORDER BY last_seen_at DESC NULLS LAST;
                       """,
                "chart": {"type": "table"},
                "tags": ["support"],
                "params": ["user_id"]
            },
            "Support: subscription history (table)": {
                "sql": """
                       SELECT s.plan_id, pl.name AS plan_name, s.status, s.start_at, s.end_at
                       FROM {S}.subscriptions s
            LEFT JOIN {S}.plans pl
                       ON pl.plan_id = s.plan_id
                       WHERE s.user_id = :user_id
                       ORDER BY s.start_at DESC;
                       """,
                "chart": {"type": "table"},
                "tags": ["support"],
                "params": ["user_id"]
            },

            # ===== Content Ops =====
            "Content Ops: watchlist adds last N days (bar)": {
                "sql": """
                       SELECT c.title, COUNT(*) ::int AS adds
                       FROM {S}.watchlist w
            JOIN {S}.content c
                       ON c.content_id = w.content_id
                       WHERE w.added_at >= NOW() - (:since_days || ' days'):: interval
                       GROUP BY c.title
                       ORDER BY adds DESC
                           LIMIT 20;
                       """,
                "chart": {"type": "bar", "x": "title", "y": "adds"},
                "tags": ["content_ops"],
                "params": ["since_days"]
            },
            "Content Ops: rating distribution for a title (bar)": {
                "sql": """
                       SELECT r.stars, COUNT(*) ::int AS cnt
                       FROM {S}.ratings r
                       WHERE r.content_id = :content_id
                       GROUP BY r.stars
                       ORDER BY r.stars;
                       """,
                "chart": {"type": "bar", "x": "stars", "y": "cnt"},
                "tags": ["content_ops"],
                "params": ["content_id"]
            },
            "Content Ops: top genres by avg stars (bar)": {
                "sql": """
                       SELECT g.name AS genre, ROUND(AVG(r.stars)::numeric, 2) AS avg_stars
                       FROM {S}.ratings r
            JOIN {S}.content_genres cg
                       ON cg.content_id = r.content_id
                           JOIN {S}.genres g ON g.genre_id = cg.genre_id
                       GROUP BY g.name
                       HAVING COUNT (*) >= 10
                       ORDER BY avg_stars DESC
                           LIMIT 15;
                       """,
                "chart": {"type": "bar", "x": "genre", "y": "avg_stars"},
                "tags": ["content_ops"]
            },
            "Content Ops: most rated titles last N days (bar)": {
                "sql": """
                       SELECT c.title, COUNT(*) ::int AS ratings
                       FROM {S}.ratings r
            JOIN {S}.content c
                       ON c.content_id = r.content_id
                       WHERE r.created_at >= NOW() - (:since_days || ' days'):: interval
                       GROUP BY c.title
                       ORDER BY ratings DESC
                           LIMIT 20;
                       """,
                "chart": {"type": "bar", "x": "title", "y": "ratings"},
                "tags": ["content_ops"],
                "params": ["since_days"]
            },
            "Content Ops: playlist adds last N days (bar)": {
                "sql": """
                       SELECT c.title, COUNT(*) ::int AS added
                       FROM {S}.playlist_items pi
            JOIN {S}.content c
                       ON c.content_id = pi.content_id
                       WHERE pi.added_at >= NOW() - (:since_days || ' days'):: interval
                       GROUP BY c.title
                       ORDER BY added DESC
                           LIMIT 20;
                       """,
                "chart": {"type": "bar", "x": "title", "y": "added"},
                "tags": ["content_ops"],
                "params": ["since_days"]
            },

            # ===== Licensing =====
            "Licensing: deals expiring in next N days (table)": {
                "sql": """
                       SELECT l.license_id, c.title, l.region_code, r.name AS region_name, l.window_end
                       FROM {S}.license  l
            JOIN {S}.content  c
                       ON c.content_id = l.content_id
                           LEFT JOIN {S}.regions r ON r.region_code = l.region_code
                       WHERE l.window_end <= NOW() + (:since_days || ' days'):: interval
                       ORDER BY l.window_end;
                       """,
                "chart": {"type": "table"},
                "tags": ["licensing"],
                "params": ["since_days"]
            },
            "Licensing: active deals by region (bar)": {
                "sql": """
                       SELECT l.region_code, COUNT(*) ::int AS active_deals
                       FROM {S}.license l
                       WHERE NOW() BETWEEN l.window_start AND l.window_end
                       GROUP BY l.region_code
                       ORDER BY active_deals DESC;
                       """,
                "chart": {"type": "bar", "x": "region_code", "y": "active_deals"},
                "tags": ["licensing"]
            },
            "Licensing: titles with no active deal (table)": {
                "sql": """
                       SELECT c.content_id, c.title
                       FROM {S}.content c
                       WHERE NOT EXISTS (
                           SELECT 1 FROM {S}.license l
                           WHERE l.content_id = c.content_id
                         AND NOW() BETWEEN l.window_start
                         AND l.window_end
                           )
                       ORDER BY c.title;
                       """,
                "chart": {"type": "table"},
                "tags": ["licensing"]
            },
            "Licensing: coverage by region for a title (table)": {
                "sql": """
                       SELECT l.region_code, r.name AS region_name, l.window_start, l.window_end
                       FROM {S}.license l
            LEFT JOIN {S}.regions r
                       ON r.region_code = l.region_code
                       WHERE l.content_id = :content_id
                       ORDER BY l.region_code;
                       """,
                "chart": {"type": "table"},
                "tags": ["licensing"],
                "params": ["content_id"]
            }
        }
    },

    "mongo": {
        "enabled": True,
        "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        "db_name": os.getenv("MONGO_DB", "streaming"),
        "queries": {
            # TS: 播放量（近24h，按小时）
            "TS: hourly views last 24h (line)": {
                "collection": "viewing_history_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}}},
                    {"$project": {"hour": {"$dateTrunc": {"date": "$ts", "unit": "hour"}}}},
                    {"$group": {"_id": "$hour", "views": {"$count": {}}}},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "line", "x": "_id", "y": "views"}
            },

            # TS: 去重观众数（近24h，按小时）
            "TS: hourly unique viewers last 24h (line)": {
                "collection": "viewing_history_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}}},
                    {"$project": {
                        "hour": {"$dateTrunc": {"date": "$ts", "unit": "hour"}},
                        "pid": "$meta.profile_id"
                    }},
                    {"$group": {"_id": "$hour", "uniq": {"$addToSet": "$pid"}}},
                    {"$project": {"unique_viewers": {"$size": "$uniq"}}},
                    {"$sort": {"_id": 1}}
                ],
                "chart": {"type": "line", "x": "_id", "y": "unique_viewers"}
            },

            # AB：近7天各 variant 曝光量
            "AB: exposure counts last 7d by variant (bar)": {
                "collection": "ab_exposure_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)}}},
                    {"$group": {"_id": "$variant", "exposures": {"$count": {}}}},
                    {"$sort": {"exposures": -1}}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "exposures"}
            },

            # AB：暴露后24h观看量闭环
            "TS: A/B exposure → views in 24h by variant (bar)": {
                "collection": "ab_exposure_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(days=7)}}},
                    {"$project": {"profile_id": "$meta.profile_id", "variant": 1, "t0": "$ts"}},
                    {"$lookup": {
                        "from": "viewing_history_ts",
                        "let": {"pid": "$profile_id", "t0": "$t0"},
                        "pipeline": [
                            {"$match": {"$expr": {"$and": [
                                {"$eq": ["$meta.profile_id", "$$pid"]},
                                {"$gte": ["$ts", "$$t0"]},
                                {"$lte": ["$ts", {"$add": ["$$t0", 24 * 3600 * 1000]}]}
                            ]}}},
                            {"$project": {"_id": 1}}
                        ],
                        "as": "post_views"
                    }},
                    {"$project": {"variant": 1, "views_24h": {"$size": "$post_views"}}},
                    {"$group": {"_id": "$variant", "total_views_24h": {"$sum": "$views_24h"}}},
                    {"$sort": {"total_views_24h": -1}}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "total_views_24h"}
            },

            # Telemetry：每个 profile 最近一次播放
            "Telemetry: latest playback per profile (table)": {
                "collection": "viewing_history_ts",
                "aggregate": [
                    {"$sort": {"ts": -1, "_id": -1}},
                    {"$group": {"_id": "$meta.profile_id", "doc": {"$first": "$$ROOT"}}},
                    {"$replaceRoot": {"newRoot": "$doc"}},
                    {"$project": {
                        "_id": 0,
                        "profile_id": "$meta.profile_id",
                        "content_id": 1,
                        "ts": 1,
                        "device": "$meta.device_id"
                    }}
                ],
                "chart": {"type": "table"}
            },

            # Telemetry：每个 device 最近一次播放
            "Telemetry: latest playback per device (table)": {
                "collection": "viewing_history_ts",
                "aggregate": [
                    {"$sort": {"ts": -1, "_id": -1}},
                    {"$group": {"_id": "$meta.device_id", "doc": {"$first": "$$ROOT"}}},
                    {"$replaceRoot": {"newRoot": "$doc"}},
                    {"$project": {
                        "_id": 0,
                        "device_id": "$meta.device_id",
                        "profile_id": "$meta.profile_id",
                        "content_id": 1,
                        "ts": 1
                    }}
                ],
                "chart": {"type": "table"}
            },

            # Telemetry：近24h 观看最多的设备 Top10
            "Telemetry: top devices by views (24h) (bar)": {
                "collection": "viewing_history_ts",
                "aggregate": [
                    {"$match": {"ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=24)}}},
                    {"$group": {"_id": "$meta.device_id", "views": {"$count": {}}}},
                    {"$sort": {"views": -1}},
                    {"$limit": 10}
                ],
                "chart": {"type": "bar", "x": "_id", "y": "views"}
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
    pg_uri = st.text_input("Postgres URI", CONFIG["postgres"]["uri"])
    mongo_uri = st.text_input("Mongo URI", CONFIG["mongo"]["uri"])
    mongo_db = st.text_input("Mongo DB name", CONFIG["mongo"]["db_name"])
    st.divider()
    auto_run = st.checkbox("Auto-run on selection change", value=False, key="auto_run_global")

    st.header("Role & Parameters")
    role = st.selectbox("User role", ["viewer", "support", "content_ops", "licensing", "all"], index=4)

    profile_id = st.text_input("profile_id (UUID)", value="")
    user_id    = st.text_input("user_id (UUID)",    value="")
    content_id = st.text_input("content_id (UUID)", value="")
    since_days = st.slider("last N days", 1, 90, 7)

    PARAMS_CTX = {
        "profile_id": profile_id.strip(),
        "user_id":    user_id.strip(),
        "content_id": content_id.strip(),
        "since_days": int(since_days),
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
