import pytz
import time
import requests
import pandas as pd
import streamlit as st
from elasticsearch import Elasticsearch
from datetime import datetime
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
    def _connect(self, **kwargs) -> Elasticsearch:
        bearer_auth = kwargs.pop("bearer_auth", "")
        return Elasticsearch(
            cloud_id=st.secrets["ELASTICSEARCH_CLOUD_ID"],
            headers={
                "Authorization": f"Bearer {bearer_auth}",
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
            print("Authentication failed:", e)
        except Exception as e:
            print("Search error:", e)
        return None


# === OIDC Auth ===
def login():
    url = f"{ELASTIC_AUTH_BASE_URL}/prepare"
    payload = {"realm": ELASTIC_AUTH_REALM, "nonce": AUTH_NONCE}
    headers = {
        "Content-Type": "application/json",
        "Authorization": ELASTIC_API_KEY,
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def logout(token: str, refresh_token: str):
    url = f"{ELASTIC_AUTH_BASE_URL}/logout"
    payload = {"token": token, "refresh_token": refresh_token}
    headers = {
        "Content-Type": "application/json",
        "Authorization": ELASTIC_API_KEY,
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def authorize(redirect_uri: str, state: str):
    url = f"{ELASTIC_AUTH_BASE_URL}/authenticate"
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
    page_title="Retail Analytics | Skylight",
)

if not st.session_state.get("access_token") and not st.query_params.get("login"):
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

if not st.session_state.get("access_token"):
    st.stop()


adelaide_tz = pytz.timezone(TIME_ZONE)
conn = st.connection(
    "es", type=ElasticConnection, bearer_auth=st.session_state["access_token"]
)


def get_total_sales(filters):
    res = conn.query(
        query="""
        SELECT
            sum("data.transaction_value.total_ex") as "total_sales" 
        FROM
            "*-retail-transactions"
        """,
        filter=filters,
    )
    return res


def get_budget(filters):
    res = conn.query(
        query='SELECT budget_total FROM "event-schedule" ORDER BY budget_total desc',
        filter=filters,
    )
    return res


def get_highest_hour(filters):
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
    return res


def get_active_terminals(filters):
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
    return res


def get_sales_by_location(filters):
    res = conn.query(
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
    return res


def get_sales_by_timestamp():
    return


def get_sales_by_product(filters):
    res = conn.query(
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
        """,
        filter=filters,
    )
    return res


def get_visitation(filters):
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
    return res


# Draw the actual page
# Set the title that appears at the top of the page.
"""
# Retail Analytics

"""

if st.button("Log out"):
    logout_response = logout(
        token=st.session_state.get("access_token"),
        refresh_token=st.session_state.get("refresh_token"),
    )

    if logout_response.get("redirect"):
        st.markdown(
            f'<meta http-equiv="refresh" content="0;url={LOGOUT_URL}">',
            unsafe_allow_html=True,
        )


date_filter = st.date_input("Pick a date", max_value="today", format="DD/MM/YYYY")

reporting_group = st.pills("Filter data", options=["event_retail", "mtx_club_hotel"])
refresh_toggle = st.toggle(value=False, label="Auto Refresh")
refresh_seconds = st.number_input(value=5, label="Refresh seconds")

filters = configure_filters(reporting_group=reporting_group, date=date_filter)

# creating a single-element container
placeholder = st.empty()

refresh = True
while refresh:

    with placeholder.container():

        f"Last refresh: {datetime.now(adelaide_tz).strftime('%A, %d %B %Y %I:%M:%S %p')}"

        total_sales = get_total_sales(filters=filters)
        budget = get_budget(configure_filters(date_filter))
        highest_hour = get_highest_hour(filters=filters)
        active_terminals = get_active_terminals(filters=filters)
        sales_by_Location = get_sales_by_location(filters=filters)
        sales_by_product = get_sales_by_product(filters)
        visitation = get_visitation(configure_filters(date_filter))

        st.write(total_sales)
        st.write(budget)
        st.write(highest_hour)
        st.write(active_terminals)
        st.write(sales_by_Location)
        st.write(sales_by_product)
        st.write(visitation)

    refresh = refresh_toggle

    if refresh:
        time.sleep(refresh_seconds)
