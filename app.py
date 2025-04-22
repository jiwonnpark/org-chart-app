import streamlit as st
import pandas as pd
import os
from databricks import sql
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables
load_dotenv()
DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
DATABRICKS_PATH = st.secrets["DATABRICKS_PATH"]
DATABRICKS_TOKEN = st.secrets["DATABRICKS_TOKEN"]

@st.cache_data
def load_data():
    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_PATH,
        access_token=DATABRICKS_TOKEN
    )
    query = """
SELECT *,
    CASE 
        WHEN management_level LIKE 'P%' OR management_level LIKE 'M%' THEN
            CASE 
                WHEN CAST(SUBSTRING(management_level, 2) AS INT) BETWEEN 20 AND 40 THEN 'Junior'
                WHEN CAST(SUBSTRING(management_level, 2) AS INT) BETWEEN 50 AND 60 THEN 'Mid-level'
                WHEN CAST(SUBSTRING(management_level, 2) AS INT) >= 70 THEN 'Senior'
                ELSE 'Unknown'
            END
        ELSE 'Unknown'
    END AS levels_summary,
    CASE 
       WHEN location LIKE 'Remote - Poland' THEN 'PL'
       WHEN location LIKE 'Remote - MN' THEN 'US'
       WHEN location LIKE 'Remote - CA' THEN 'US'
       WHEN location LIKE 'Remote - Seattle' THEN 'US'
       WHEN location LIKE 'Remote - VA Metro' THEN 'US'
       WHEN location LIKE 'London - UK2' THEN 'UK'
       WHEN location LIKE 'Remote - MA' THEN 'US'
       WHEN location LIKE 'San Francisco - SF9' THEN 'US'
       WHEN location LIKE 'Bengaluru - BLR1' THEN 'IN'
       WHEN location LIKE 'Remote - NYC' THEN 'US'
       WHEN location LIKE 'Remote - SF Bay Area' THEN 'US'
       WHEN location LIKE 'Remote - Houston' THEN 'US'
       WHEN location LIKE 'Remote - FL' THEN 'US'
       WHEN location LIKE 'Remote - Dallas' THEN 'US'
       WHEN location LIKE 'Remote - Chicago' THEN 'US'
       WHEN location LIKE 'Remote - WI' THEN 'US'
       WHEN location LIKE 'Remote - NC' THEN 'US'
       WHEN location LIKE 'Remote - HI' THEN 'US'
       WHEN location LIKE 'Remote - Netherlands' THEN 'NL'
       WHEN location LIKE 'Remote - NY' THEN 'US'
       WHEN location LIKE 'Remote - WA' THEN 'US'
       WHEN location LIKE 'Atlanta - ATL2' THEN 'US'
       WHEN location LIKE 'Remote - Montreal' THEN 'CA'
       WHEN location LIKE 'Remote - Austin' THEN 'US'
       WHEN location LIKE 'Remote - GA' THEN 'US'
       WHEN location LIKE 'Remote - Mexico' THEN 'MX'
       WHEN location LIKE 'Remote - Toronto' THEN 'CA'
       WHEN location LIKE 'Remote - Vancouver' THEN 'CA'
       WHEN location LIKE 'Remote - UK' THEN 'GB'
       WHEN location LIKE 'Remote - Denver' THEN 'US'
       WHEN location LIKE 'Remote - PA' THEN 'US'
       WHEN location LIKE 'Remote - NJ Metro' THEN 'US'
       WHEN location LIKE 'Remote - MO' THEN 'US'
       WHEN location LIKE 'Remote - NJ' THEN 'US'
       WHEN location LIKE 'Remote - OR' THEN 'US'
       WHEN location LIKE 'Remote - NV' THEN 'US'
       WHEN location LIKE 'Remote - CO' THEN 'US'
       WHEN location LIKE 'Remote - Canada' THEN 'CA'
       WHEN location LIKE 'Remote - VA' THEN 'US'
       WHEN location LIKE 'Remote - MI' THEN 'US'
       WHEN location LIKE 'Remote - AZ' THEN 'US'
       WHEN location LIKE 'Remote - CT' THEN 'US'
       WHEN location LIKE 'Remote - UT' THEN 'US'
       WHEN location LIKE 'Remote - Taiwan' THEN 'TW'
       WHEN location LIKE 'Remote - OH' THEN 'US'
       WHEN location LIKE 'Remote - ND' THEN 'US'
       WHEN location LIKE 'TW1' THEN 'TW'
       WHEN location LIKE 'Remote - KS' THEN 'US'
       WHEN location LIKE 'Remote - TN' THEN 'US'
       WHEN location LIKE 'Remote - MD' THEN 'US'
       WHEN location LIKE 'Remote - NE' THEN 'US'
       WHEN location LIKE 'Remote - DC' THEN 'US'
       WHEN location LIKE 'Remote - MD Metro' THEN 'US'
       WHEN location LIKE 'Remote - Calgary' THEN 'CA'
       WHEN location LIKE 'Remote - IL' THEN 'US'
       WHEN location LIKE 'Remote - TX' THEN 'US'
       WHEN location LIKE 'Remote - IA' THEN 'US'
       WHEN location LIKE 'CDMX3' THEN 'MX'
       WHEN location LIKE 'Remote - MT' THEN 'US'
       WHEN location LIKE 'Remote - OK' THEN 'US'
       WHEN location LIKE 'Remote - AK' THEN 'US'
       WHEN location LIKE 'Remote - AL' THEN 'US'
       WHEN location LIKE 'Remote - France' THEN 'FR'
       WHEN location LIKE 'Remote - NH' THEN 'US'
       WHEN location LIKE 'Remote - Wyoming' THEN 'US'
       WHEN location LIKE 'Remote - Edmonton' THEN 'CA'
       WHEN location LIKE 'Phoenix - PHX1' THEN 'US'
       ELSE 'Unknown'
    END AS country,
    employee_id,
    adjusted_preferred_name AS employee_name,
    hire_date,
    CURRENT_DATE AS today,
    DATEDIFF(CURRENT_DATE, hire_date) AS tenure,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, hire_date) <= 0 THEN 'SNS'
        ELSE 'BIS'
    END AS hc_type
FROM edw.sensitive_silver.workday_employee_roster
WHERE department = '100000 - Engineering' 
  AND report_effective_date = (
    SELECT MAX(report_effective_date)
    FROM edw.sensitive_silver.workday_employee_roster
    WHERE department = '100000 - Engineering'
  );
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def format_tenure(days):
    y = days // 365
    m = round((days % 365) / 30)
    if m == 12:
        y += 1
        m = 0
    return f"{y} y, {m} mo"

def build_tree(df):
    info_map = {}
    tree = defaultdict(list)
    for _, row in df.iterrows():
        name = row['employee_name']
        manager = row['manager_final']
        info_map[name] = {
            "name": name,
            "manager": manager,
            "hc_type": row["hc_type"],
            "location": row["country"],
            "level": row["management_level"],
            "tenure": format_tenure(int(row["tenure"])),
            "bis": 0,
            "sns": 0,
            "total": 0,
        }
        tree[manager].append(name)
    return info_map, tree

def count_rollups(manager, info_map, tree):
    if manager not in tree:
        return 0, 0
    bis = sns = 0
    for emp in tree[manager]:
        b, s = count_rollups(emp, info_map, tree)
        bis += b
        sns += s
        if info_map[emp]["hc_type"] == "BIS":
            bis += 1
        elif info_map[emp]["hc_type"] == "SNS":
            sns += 1
    info_map[manager]["bis"] = bis
    info_map[manager]["sns"] = sns
    info_map[manager]["total"] = bis + sns
    return bis, sns


def count_country_breakdown(manager, info_map, tree):
    from collections import Counter
    if manager not in tree:
        return {}
    counts = Counter()
    for emp in tree[manager]:
        counts[info_map[emp]["location"]] += 1
        if emp in tree:
            sub_counts = count_country_breakdown(emp, info_map, tree)
            for k, v in sub_counts.items():
                counts[k] += v
    return dict(counts)

def build_summary(manager, info_map, tree, level=0):
    rows = []
    if manager not in info_map:
        return rows
    data = info_map[manager]
    country_counts = count_country_breakdown(manager, info_map, tree)
    rows.append({
        "Manager": manager,
        "Level": data["level"],
        "BIS": data["bis"],
        "SNS": data["sns"],
        "Total": data["total"], **country_counts,
        "Indent": level
    })
    for emp in sorted(tree.get(manager, []), key=lambda e: info_map[e]["total"], reverse=True):
        if emp in tree:
            rows.extend(build_summary(emp, info_map, tree, level + 1))
    return rows

def render_html_chart(manager, info_map, tree, search_filter=None, location_filter=None):
    if manager not in tree:
        return ""
    html = "<ul>"
    for emp in sorted(tree[manager], key=lambda e: info_map[e]["total"], reverse=True):
        data = info_map[emp]
        if location_filter and data["location"] != location_filter:
            continue
        if search_filter and search_filter.lower() not in emp.lower():
            if emp not in tree:
                continue
            subtree = render_html_chart(emp, info_map, tree, search_filter, location_filter)
            if subtree.strip() == "<ul></ul>":
                continue
        arrow = "<span class='arrow'>▶</span>" if emp in tree else ""
        summary = f" | BIS: {data['bis']} | SNS: {data['sns']} | Total: {data['total']}" if emp in tree else ""
        if emp in tree:
            html += f"<li><span class='node'>{arrow}<strong>{emp}</strong> | {data['hc_type']} | {data['location']} | {data['level']} | {data['tenure']}{summary}</span>" + render_html_chart(emp, info_map, tree, search_filter, location_filter) + "</li>"
        else:
                glean_link = f"<a href='https://app.glean.com/search?q={emp}' target='_blank' title='Search Glean'>🔍</a>"
                html += f"<li><span class='node'><strong>{emp}</strong> | {data['hc_type']} | {data['location']} | {data['level']} | {data['tenure']} {glean_link}</span></li>"
    html += "</ul>"
    return html

# Streamlit UI
st.set_page_config(layout="wide")
st.title("R&D Headcount Summary")

df = load_data()
df['report_effective_date'] = pd.to_datetime(df['report_effective_date'])
effective_date = df['report_effective_date'].max().strftime('%Y-%m-%d')
effective_date = df['report_effective_date'].max().strftime('%Y-%m-%d')
locations = sorted(df["country"].dropna().unique())
vp_leaders = df[df['management_level'].str.extract(r'(\d+)').astype(float).fillna(0)[0] >= 90]['employee_name'].dropna().unique()

sorted_leaders = sorted(vp_leaders)
root_query = st.selectbox(
    "👤 Choose VP+ Leader",
    options=sorted_leaders,
    index=sorted_leaders.index("Kiren Sekar") if "Kiren Sekar" in sorted_leaders else 0
)


# Filter DataFrame
filtered_df = df.copy()

info_map, tree = build_tree(filtered_df)
if root_query in info_map:
    root_person = root_query
else:
    root_person = "Kiren Sekar"
count_rollups(root_person, info_map, tree)
# Load open job requisitions
@st.cache_data
def load_open_reqs():
    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_PATH,
        access_token=DATABRICKS_TOKEN
    )
    query = """
    SELECT job_name, job_id, geo_zone, hiring_managers,
           hiring_managers[0] AS hiring_manager_name
    FROM edw.sensitive_silver.greenhouse_job_openings
    WHERE financial_department = '100000 - Engineering'
      AND job_opening_status = 'open'
      AND employment_type != 'Intern'
    """
    return pd.read_sql(query, conn)

open_reqs_df = load_open_reqs()

# Determine all managers in scope
def get_all_managers(manager, tree):
    all_managers = set()
    if manager in tree:
        for emp in tree[manager]:
            all_managers.add(emp)
            all_managers.update(get_all_managers(emp, tree))
    return all_managers

visible_managers = get_all_managers(root_person, tree)
visible_managers.add(root_person)
open_reqs_filtered = open_reqs_df[open_reqs_df["hiring_manager_name"].isin(visible_managers)].copy()

# Add Greenhouse link
if "job_id" in open_reqs_filtered.columns:
    open_reqs_filtered["Greenhouse"] = open_reqs_filtered["job_id"].apply(
        lambda x: f"<a href='https://samsara.greenhouse.io/sdash/{x}' target='_blank'>🔗</a>"
    )

# Display selected columns only
display_columns = ["job_name", "job_id", "geo_zone", "hiring_managers", "Greenhouse"]



summary_df = pd.DataFrame(build_summary(root_person, info_map, tree))
# Enhance summary_df with Open Reqs
if not open_reqs_filtered.empty:
    open_reqs_by_manager = open_reqs_filtered["hiring_manager_name"].value_counts().to_dict()
    summary_df["Open Reqs"] = summary_df["Manager"].map(open_reqs_by_manager).fillna(0).astype(int)
else:
    summary_df["Open Reqs"] = 0

summary_df["BIS+SNS"] = summary_df["BIS"] + summary_df["SNS"]
summary_df["BIS+SNS+Open Reqs"] = summary_df["BIS+SNS"] + summary_df["Open Reqs"]

with st.expander("📊 Summary Table (BIS, SNS & Open Reqs)"):
    if "Indent" in summary_df.columns:
        for i in summary_df["Indent"].unique():
            summary_df.loc[summary_df["Indent"] == i, "Manager"] = (
                "&nbsp;" * (4 * i) + summary_df.loc[summary_df["Indent"] == i, "Manager"]
            )
    st.markdown(summary_df[["Manager", "Level", "BIS", "SNS", "BIS+SNS", "Open Reqs", "BIS+SNS+Open Reqs"]].to_html(escape=False, index=False), unsafe_allow_html=True)

with st.expander("🌍 Summary Table (By Country BIS+SNS)"):
    if not summary_df.empty:
        country_cols = [col for col in summary_df.columns if col not in ["Manager", "Level", "BIS", "SNS", "Total", "Indent", "Open Reqs", "BIS+SNS", "BIS+SNS+Open Reqs"]]
        country_df = summary_df[["Manager", "Level", "Total"] + country_cols].copy()
        country_df[country_cols] = country_df[country_cols].fillna(0).astype(int)
        st.markdown(country_df.to_html(escape=False, index=False), unsafe_allow_html=True)






with st.expander("🧭 Org Chart", expanded=True):
    root_data = info_map[root_person]
    root = f"<span class='node'><span class='arrow'>▶</span><strong>{root_data['name']}</strong> | {root_data['hc_type']} | {root_data['location']} | {root_data['level']} | {root_data['tenure']} | BIS: {root_data['bis']} | SNS: {root_data['sns']} | Total: {root_data['total']}</span>"
    html = f"""
    
    <link href="https://fonts.googleapis.com/css2?family=Inter&display=swap" rel="stylesheet">
    <style>
        body, ul, li, .node {{
            font-family: 'Inter', sans-serif;
            font-size: 15px;
        }}
        ul {{ list-style-type: none; padding-left: 20px; }}
        li {{ margin: 5px 0; cursor: pointer; }}
        .node {{ padding: 5px 10px; border-radius: 5px; display: inline-block; }}
        .collapsed > ul {{ display: none; }}
        .node:hover {{ background-color: #f0f0f0; }}
        .arrow {{ display: inline-block; width: 1em; transition: transform 0.2s ease; margin-right: 5px; }}
        .collapsed > .node > .arrow {{ transform: rotate(0deg); }}
        li:not(.collapsed) > .node > .arrow {{ transform: rotate(90deg); }}
    </style>

    <ul><li>{root}{render_html_chart(root_person, info_map, tree, None, None)}</li></ul>
    <script>
        document.querySelectorAll("li").forEach(li => {{
            if (li.querySelector("ul")) {{
                li.classList.add("collapsed");
                li.addEventListener("click", function(e) {{
                    e.stopPropagation();
                    this.classList.toggle("collapsed");
                }});
            }}
        }});
    </script>
    <p style='margin-top: 2em; font-style: italic; color: gray; text-align: center;'>
        Org chart reflects data as of {effective_date}
    </p>
    """
    st.components.v1.html(html, height=1000, scrolling=True)



with st.expander("📋 Open Reqs"):
    if not open_reqs_filtered.empty:
        st.markdown(open_reqs_filtered[["job_name", "job_id", "geo_zone", "hiring_managers", "Greenhouse"]].to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.write("No open requisitions for this org.")