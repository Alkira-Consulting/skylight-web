import pytz
import requests
import pandas as pd
import streamlit as st
from elasticsearch import Elasticsearch
from zoneinfo import ZoneInfo
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from streamlit_extras.stylable_container import stylable_container
from elasticsearch.exceptions import AuthenticationException
from streamlit.connections import ExperimentalBaseConnection

# === Constants ===
ELASTIC_CLOUD_ID = st.secrets["ELASTICSEARCH_CLOUD_ID"]
ELASTIC_AUTH_REALM = st.secrets["ELASTIC_AUTH_REALM"]
ELASTIC_API_KEY = st.secrets["ELASTIC_API_KEY"]
ELASTIC_AUTH_BASE_URL = st.secrets["ELASTIC_AUTH_BASE_URL"]
LOGOUT_URL = st.secrets["LOGOUT_URL"]
AUTH_NONCE = st.secrets["AUTH_NONCE"]
TIME_ZONE = st.secrets["TIME_ZONE"]


# === Elasticsearch Connection Wrapper ===
class ElasticConnection(ExperimentalBaseConnection[Elasticsearch]):
    def __init__(self, connection_name: str, access_token: str, **kwargs):
        self.access_token = access_token
        super().__init__(connection_name, **kwargs)

    def _connect(self) -> Elasticsearch:
        return Elasticsearch(
            cloud_id=st.secrets["ELASTICSEARCH_CLOUD_ID"],
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/vnd.elasticsearch+json;compatible-with=8",
                "Accept": "application/vnd.elasticsearch+json;compatible-with=8",
            },
        )

    @property
    def client(self) -> Elasticsearch:
        return self._instance

    def query(self, query, filter):
        try:
            # Run the query
            res = self.client.sql.query(
                query=query,
                filter=filter,
            )

            # Extract column names and rows
            columns = [col["name"] for col in res["columns"]]
            rows = res["rows"]

            # Convert to DataFrame
            return pd.DataFrame(rows, columns=columns)
        except AuthenticationException as e:
            pass
        except Exception as e:
            print("Search error:", e)
        return None

    def get_info(self):
        try:
            return self.client.info()
        except:
            return None


# === OIDC Auth ===
def login():
    url = f"{ELASTIC_AUTH_BASE_URL}/_security/oidc/prepare"
    payload = {"realm": ELASTIC_AUTH_REALM, "nonce": AUTH_NONCE}
    headers = {
        "Content-Type": "application/json",
        "Authorization": ELASTIC_API_KEY,
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def logout(token: str, refresh_token: str):
    url = f"{ELASTIC_AUTH_BASE_URL}/_security/oidc/logout"
    payload = {"token": token, "refresh_token": refresh_token}
    headers = {
        "Content-Type": "application/json",
        "Authorization": ELASTIC_API_KEY,
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def authorize(redirect_uri: str, state: str):
    url = f"{ELASTIC_AUTH_BASE_URL}/_security/oidc/authenticate"
    payload = {
        "redirect_uri": redirect_uri,
        "state": state,
        "nonce": AUTH_NONCE,
        "realm": ELASTIC_AUTH_REALM,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": ELASTIC_API_KEY,
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def refresh(refresh_token: str):
    url = f"{ELASTIC_AUTH_BASE_URL}/_security/oauth2/token"
    payload = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    headers = {
        "Content-Type": "application/json",
        "Authorization": ELASTIC_API_KEY,
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def configure_filters(date=None, reporting_group=None):
    filters = []

    if not date:
        date = "today/d"

    filters.append(
        {
            "range": {
                "@timestamp": {
                    "gte": date,
                    "lte": date,
                    "time_zone": TIME_ZONE,
                }
            }
        }
    )

    if reporting_group:
        filters.append({"term": {"reporting.reporting_group": reporting_group}})

    return {"bool": {"must": filters}}


# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title="AO Retail Analytics | Alkira Skylight",
)


def logout_user():
    logout(
        token=st.session_state.get("access_token"),
        refresh_token=st.session_state.get("refresh_token"),
    )

    del st.session_state["access_token"]
    del st.session_state["refresh_token"]

    return st.markdown(
        f'<meta http-equiv="refresh" content="0;url={LOGOUT_URL}">',
        unsafe_allow_html=True,
    )


if (
    not st.session_state.get("access_token")
    and not st.session_state.get("refresh_token")
    and not st.query_params.get("login")
):
    login_response = login()

    if login_response.get("redirect") and login_response.get("nonce"):
        st.markdown(
            f'<meta http-equiv="refresh" content="0;url={login_response["redirect"]}">',
            unsafe_allow_html=True,
        )
    else:
        st.write("Something went wrong with authentication")


if st.query_params.get("login") == "true":
    auth_code = st.query_params.get("code")
    state = st.query_params.get("state")

    if auth_code and state:
        redirect_uri = f"www/?code={auth_code}&state={state}"
        user = authorize(redirect_uri=redirect_uri, state=state)

        if user.get("access_token") and user.get("refresh_token"):
            st.session_state["access_token"] = user["access_token"]
            st.session_state["refresh_token"] = user["refresh_token"]

        st.query_params.clear()

if not st.session_state.get("access_token") and not st.session_state.get(
    "refresh_token"
):
    st.stop()


adelaide_tz = pytz.timezone(TIME_ZONE)
conn = st.connection(
    "es", type=ElasticConnection, access_token=st.session_state["access_token"]
)

if conn.get_info() is None:
    try:
        refresh_res = refresh(st.session_state.get("refresh_token"))

        if refresh_res.get("error"):
            logout_user()

        st.session_state["access_token"] = refresh_res.get("access_token")
        st.session_state["refresh_token"] = refresh_res.get("refresh_token")
        conn = st.connection(
            "es", type=ElasticConnection, access_token=st.session_state["access_token"]
        )
    except:
        logout_user()


def total_sales_metric(filters):
    res = conn.query(
        query="""
        SELECT
            sum("data.transaction_value.total_ex") as "total_sales" 
        FROM
            "*-retail-transactions"
        """,
        filter=filters,
    )

    try:
        total_sales = res.loc[0, "total_sales"]
        total_sales = f"${round(total_sales):,}"

    except:
        total_sales = "-"

    return st.metric(
        label="Total Sales (ex GST)",
        value=total_sales,
        border=True,
    )


def highest_hour_metric(filters):
    res = conn.query(
        query="""
        SELECT 
            HISTOGRAM("@timestamp",INTERVAL 1 HOUR) as "datetime", 
            sum("data.transaction_value.total_ex") as "sale_total",
            count(DISTINCT "data.id") as "txn_total" 
        FROM
            "*-retail-transactions" 
        WHERE 
            "data.transaction_value.total_ex" != 0 
        GROUP BY 
            "datetime" 
        ORDER BY 
            sum("data.transaction_value.total_ex") desc 
        LIMIT 1
        """,
        filter=filters,
    )

    try:
        hour = res.loc[0, "datetime"]
        hour_utc = datetime.fromisoformat(hour.replace("Z", "+00:00"))
        adelaide_time = hour_utc.astimezone(ZoneInfo("Australia/Adelaide"))
        highest_hour = adelaide_time.strftime("%-I %p")
    except:
        highest_hour = "-"

    return st.metric(label="Highest Hour", value=highest_hour, border=True)


def active_terminals_metric(filters):
    res = conn.query(
        query="""
        SELECT 
            count(DISTINCT "data.transaction.terminal.id") as "swiftpos_terminals",
            count(DISTINCT "data.transaction.kiosk_id") as "mashgin_terminals"
        FROM
            "*-retail-product"
        WHERE
            "@timestamp" > DATEADD('minutes', -15, NOW()) and "data.total_ex" != 0
        """,
        filter=filters,
    )

    try:
        active_terminals = (
            res.loc[0, "swiftpos_terminals"] + res.loc[0, "mashgin_terminals"]
        )
    except:
        active_terminals = "-"

    return st.metric(label="Active Terminals", value=active_terminals, border=True)


def sales_by_location_dataframe(filters):
    sales_by_Location = conn.query(
        query="""
        SELECT
            "data.location.name" as "Location",
            count(DISTINCT CASE 
                WHEN "@timestamp" > DATEADD('minutes', -15, NOW()) THEN "data.transaction.terminal.id"
                ELSE null
            END) as "Active",
            sum(CASE 
                WHEN "data.master_group.id" = '20' THEN "data.total_ex"
                ELSE 0
            END) as "Beverage",
            sum(CASE 
                WHEN "data.master_group.id" = '10' THEN "data.total_ex"
                ELSE 0
            END) as "Food",
            sum("data.total_ex") as "Total"
        FROM 
            "*-retail-product"
        WHERE 
            "data.total_ex" != 0 and "data.master_group.id" = '10' or "data.master_group.id" = '20'
        GROUP BY
            "data.location.id", "data.location.name"
        ORDER BY
            sum("data.total_ex") desc
        LIMIT 15
        """,
        filter=filters,
    )

    if sales_by_Location is not None and not sales_by_Location.empty:

        sales_by_Location = sales_by_Location.style.format(
            {
                "Beverage": lambda x: f"${x:,.0f}",
                "Food": lambda x: f"${x:,.0f}",
                "Total": lambda x: f"${x:,.0f}",
            }
        )

        return st.dataframe(sales_by_Location, hide_index=True)


def sales_bar_chart(filters):
    sales_by_timestamp = conn.query(
        query="""
    SELECT 
        HISTOGRAM("@timestamp",INTERVAL 1 MINUTE) as "datetime", 
        sum("data.transaction_value.total_ex") as "sale_total"
    FROM
        "*-retail-transactions" 
    WHERE 
        "data.transaction_value.total_ex" != 0 
    GROUP BY 
        "datetime" 
    ORDER BY 
        sum("data.transaction_value.total_ex") desc 
    """,
        filter=filters,
    )

    try:
        sales_by_timestamp["datetime"] = pd.to_datetime(
            sales_by_timestamp["datetime"], utc=True
        )
        sales_by_timestamp["datetime"] = sales_by_timestamp["datetime"].dt.tz_convert(
            "Australia/Adelaide"
        )
    except:
        return

    if sales_by_timestamp.empty:
        return

    return st.bar_chart(
        sales_by_timestamp,
        x="datetime",
        y="sale_total",
        x_label="Time",
        y_label="Sales (ex GST)",
    )


def sales_by_product_dataframe(filters):
    sales_by_product = conn.query(
        query="""
        SELECT
            "data.name.keyword" as "Item",
            sum("data.quantity") as "Qty Sold",
            sum("data.total_ex") as "Total"
        FROM
            "swiftpos-retail-product"
        WHERE 
            "data.master_group.id" = '10' or "data.master_group.id" = '20'
        GROUP BY 
            "data.name.keyword"
        ORDER BY 
            sum("data.total_ex") desc
        LIMIT 20
        """,
        filter=filters,
    )

    if sales_by_product is not None and not sales_by_product.empty:

        sales_by_product = sales_by_product.style.format(
            {"Qty Sold": "{:,.0f}", "Total": lambda x: f"${x:,.0f}"}
        )

        return st.dataframe(sales_by_product, hide_index=True)


def visitation_metric(filters):
    res = conn.query(
        query="""
        SELECT 
            count("data.barcode") as entries
        FROM
            "ticketek-customer-attendance"
        WHERE
            "data.status.type" = 'Entry' and "data.priceTypeName" != 'TICKETEK TEST'
        """,
        filter=filters,
    )

    try:
        visitation = res.loc[0, "entries"]
        visitation = f"{visitation:,}"
    except:
        visitation = "-"

    return st.metric(label="Visitors", value=visitation, border=True)


# Draw the actual page
# Set the title that appears at the top of the page.
with stylable_container(
    key="logout",
    css_styles="""
    button{
        float: right;
    }
    """,
):
    with st.popover("", icon=":material/settings:"):
        refresh = st.toggle(value=True, label="Auto Refresh")
        refresh_seconds = st.number_input(value=10, label="Refresh seconds")
        if st.button("Logout", icon=":material/logout:", type="tertiary"):
            logout_response = logout(
                token=st.session_state.get("access_token"),
                refresh_token=st.session_state.get("refresh_token"),
            )
            logout_user()

st.title("Skylight")

date_filter = st.date_input("Pick a date", max_value="today", format="DD/MM/YYYY")

reporting_group = st.pills(
    "Filter data", options=["event_retail", "mtx_club_hotel"], default="event_retail"
)

if refresh:
    st_autorefresh(interval=refresh_seconds * 1000, key="auto_refresh")

filters = configure_filters(reporting_group=reporting_group, date=date_filter)

col1, col2 = st.columns(2)

with col1:
    total_sales_metric(filters=filters)
    highest_hour_metric(filters=filters)

with col2:
    visitation_metric(filters=configure_filters(date_filter))
    active_terminals_metric(filters=filters)

sales_bar_chart(filters=filters)

st.subheader("Top Locations")
sales_by_location_dataframe(filters=filters)

st.subheader("Top Products")
sales_by_product_dataframe(filters=filters)


st.caption(
    f"Last refresh: {datetime.now(adelaide_tz).strftime('%A, %d %B %Y %I:%M:%S %p')}"
)

st.badge("Powered by Alkira Skylight", color="blue")


st.markdown(
    """
    <style>
        div[data-testid="column"]:nth-of-type(1)
        {
            border:1px solid red;
        } 

        div[data-testid="column"]:nth-of-type(2)
        {
            border:1px solid blue;
            text-align: end;
        } 
    </style>
    """,
    unsafe_allow_html=True,
)
