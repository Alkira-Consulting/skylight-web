import streamlit as st
from streamlit.connections import ExperimentalBaseConnection
from elasticsearch import Elasticsearch
import pandas as pd
from pandas import DataFrame, json_normalize
from datetime import datetime, time, timedelta
import pytz
import time


class ElasticConnection(ExperimentalBaseConnection[Elasticsearch], Elasticsearch):
    def _connect(self, **kwargs) -> Elasticsearch:
        if "cloud_id" in kwargs and "api_key" in kwargs:
            cloud_id = kwargs.pop("cloud_id")
            api_key = kwargs.pop("api_key")
        else:
            cloud_id = st.secrets["ELASTICSEARCH_CLOUD_ID"]
            api_key = st.secrets["ELASTICSEARCH_API_KEY"]

        return Elasticsearch(cloud_id=cloud_id, api_key=api_key)

    @property
    def client(self) -> Elasticsearch:
        return self._instance

    def search(
        self,
        index: str,
        query: dict[str, any] = {},
        sort: dict[str, any] = None,
        source: list[str] = None,
        size: int = None,
        aggs: dict[str, any] = None,
    ) -> DataFrame:
        search = self.client.search(
            index=index, query=query, sort=sort, source=source, size=size, aggs=aggs
        )

        return json_normalize([x["_source"] for x in search["hits"]["hits"]])

    def searchv2(
        self,
        index: str,
        query: dict[str, any] = {},
        sort: dict[str, any] = None,
        source: list[str] = None,
        size: int = None,
        aggs: dict[str, any] = None,
    ) -> DataFrame:
        return self.client.search(
            index=index, query=query, sort=sort, source=source, size=size, aggs=aggs
        )

    def search_aggregation(
        self,
        index: str,
        aggregations: dict[str, any],
        query: dict[str, any] = None,
    ) -> DataFrame:
        search = self.client.search(
            index=index, aggregations=aggregations, query=query, size=0
        )

        aggs = []

        for aggregation in aggregations:
            if "buckets" in search["aggregations"][aggregation]:
                buckets = search["aggregations"][aggregation]["buckets"]

                df = pd.DataFrame(
                    [
                        {
                            "bucket": bucket["key_as_string"],
                            "value": bucket["total_ex_sum"]["value"],
                        }
                        for bucket in buckets
                    ]
                )

                return df

            else:
                aggs.append(
                    {
                        "aggregation": aggregation,
                        "value": search["aggregations"][aggregation]["value"],
                    }
                )

        return pd.DataFrame(aggs)


conn = st.connection("ao_elastic", type=ElasticConnection)
adelaide_tz = pytz.timezone("Australia/Adelaide")
time_filters = [
    {"name": "15 min", "minute_offset": 15},
    {"name": "30 min", "minute_offset": 30},
    {"name": "1 hr", "minute_offset": 60},
    {"name": "2 hr", "minute_offset": 120},
    {"name": "3 hr", "minute_offset": 180},
]

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title="Retail Analytics | Skylight",
)


# -----------------------------------------------------------------------------
# Declare some useful functions.
def relative_time_query_builder(
    index: str, offset_minutes: int = None, reporting_group: str = None, filters: list[dict[str, any]] = []
):
    if offset_minutes is None:
        time_query = {
            "bool": {
                "filter": filters
                + [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": "now/d",
                                "time_zone": "Australia/Adelaide",
                            }
                        }
                    },
                ],
            }
        }

        timestamp_search = conn.search(
            index=index,
            query=time_query,
            sort=[{"@timestamp": "asc"}],
            size=1,
            source=["@timestamp"],
        )

        first_timestamp = pd.to_datetime(timestamp_search["@timestamp"])[0]

        offset_minutes = round(
            (datetime.now(pytz.timezone("utc")) - first_timestamp).total_seconds() / 60
        )

        filters_list = filters + [
            {
                "range": {
                    "@timestamp": {
                        "gte": f"now-{offset_minutes}m",
                        "time_zone": "Australia/Adelaide",
                    }
                }
            }
        ]
        
    # Add reporting_group filter only if a value is provided
    if reporting_group:
        filters_list.append({"term": {"reporting.reporting_group.keyword": reporting_group}})

    return {
        "bool": {
            "filter": filters_list
        }
    }


def revenue_aggregations(offset_minutes: int = None, reporting_group: str = None) -> DataFrame:
    query = relative_time_query_builder(
        index="*-retail-product",
        offset_minutes=offset_minutes,
        reporting_group=reporting_group,
        filters=[{"term": {"data.transaction.standard_sale": True}}],
    )

    aggregations_df = conn.search_aggregation(
        index="*-retail-product",
        aggregations={
            "total_ex_sum": {"sum": {"field": "data.total_ex"}},
            "total_price_sum": {"sum": {"field": "data.total_price"}},
        },
        query=query,
    )

    aggregations_df = aggregations_df.set_index("aggregation")

    return aggregations_df


def revenue_chart(offset_minutes: int = None, reporting_group: str = None) -> DataFrame:
    query = relative_time_query_builder(
        index="*-retail-transactions",
        offset_minutes=offset_minutes,
        reporting_group=reporting_group,
        filters=[
            {
                "bool": {
                    "must_not": {
                        "bool": {
                            "should": [
                                {"match": {"data.transaction_value.total_inc": "0"}}
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                }
            }
        ],
    )

    return conn.search_aggregation(
        index="*-retail-transactions",
        aggregations={
            "sum_per_5min": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "5m",
                    "time_zone": "Australia/Adelaide",
                    "format": "HH:mm",
                },
                "aggs": {
                    "total_ex_sum": {
                        "sum": {"field": "data.transaction_value.total_ex"}
                    }
                },
            },
        },
        query=query,
    )


def top_locations(offset_minutes: int = None, reporting_group: str = None):
    query = relative_time_query_builder(
        index="*-retail-product",
        offset_minutes=offset_minutes,
        reporting_group=reporting_group
    )

    return pd.json_normalize(
        conn.searchv2(
            index="*-retail-product",
            query={
                "bool": {
                    "must": query,  # Keep existing query conditions
                    "filter": [
                        {
                            "bool": {
                                "should": [
                                    {"range": {"data.total_ex": {"lt": 0}}},  # Include negatives
                                    {"range": {"data.total_ex": {"gt": 0}}},  # Include positives
                                ],
                                "minimum_should_match": 1,  # At least one condition must match
                            }
                        }
                    ]
                }
            },
            size=0,
            aggs={
                "categories": {
                    "composite": {
                        "size": 20,
                        "sources": [
                            {"category": {"terms": {"field": "data.location.id"}}},
                            {
                                "sub_category": {
                                    "terms": {"field": "data.location.name.keyword"}
                                }
                            },
                        ],
                    },
                    "aggs": {
                        "total": {"sum": {"field": "data.total_ex"}},
                        "sorted_categories": {
                            "bucket_sort": {
                                "sort": [{"total": {"order": "desc"}}],
                                "size": 20
                            }
                        }
                    },
                }
            },
        )["aggregations"]["categories"]["buckets"]
    )


# -----------------------------------------------------------------------------
# Draw the actual page

# Set the title that appears at the top of the page.


"""
# Retail Analytics

"""
# top-level filters
time_filer = st.pills(
    "Show data from", options=time_filters, format_func=lambda x: x["name"]
)
reporting_group=st.pills("Filter data", options=["event_retail", "mtx_club_hotel"])
refresh_toggle = st.toggle(value=False, label="Auto Refresh")
refresh_seconds = st.number_input(value=5, label="Refresh seconds")

# creating a single-element container
placeholder = st.empty()

minute_offset = None
if time_filer:
    minute_offset = time_filer["minute_offset"]


refresh = True
while refresh:

    with placeholder.container():

        f"Last refresh: {datetime.now(adelaide_tz).strftime('%A, %d %B %Y %I:%M:%S %p')}"

        revenue_totals = revenue_aggregations(minute_offset, reporting_group=reporting_group)

        st.metric(
            label="Sales (Ex GST)",
            value=f"${revenue_totals.loc['total_ex_sum']['value']:,.2f}",
        )

        transactions_timeseries_df = revenue_chart(minute_offset, reporting_group=reporting_group)

        if not transactions_timeseries_df.empty:
            transactions_timeseries_df["value"] = round(
                transactions_timeseries_df["value"], 2
            )
            st.bar_chart(transactions_timeseries_df, x="bucket", y="value")

        top_locations_data = top_locations(offset_minutes=minute_offset, reporting_group=reporting_group)
        if "total.value" in top_locations_data: 
            top_locations_data.sort_values(by="total.value", ascending=False, inplace=True)
            top_locations_data.drop(columns=["doc_count", "key.category"], inplace=True)
            top_locations_data["total.value"] = top_locations_data["total.value"].apply(
                lambda x: f"${x:,.2f}"
            )
            top_locations_data.rename(
                columns={"key.sub_category": "Location", "total.value": "Total Sales"},
                inplace=True,
            )

        st.table(top_locations_data)

    refresh = refresh_toggle

    if refresh:
        time.sleep(refresh_seconds)
