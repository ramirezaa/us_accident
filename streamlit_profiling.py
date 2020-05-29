import pandas as pd
import os
from db_manager import *
import csv
import numpy as np
import altair as alt

import streamlit as st
import altair as alt
from vega_datasets import data
# import geopandas as gpd
from scipy.stats import spearmanr
from numpy.random import rand
from numpy.random import seed
# from matplotlib import pyplot

def get_column_names():

    query = """
        SELECT 
            distinct(column_name) as column_names
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name   = 'us_accidents';
    """

    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)
    db_object.close()
    return list(df['column_names'])

def column_pct_missing():
    col_list = get_column_names()
    db_object = DBConnect()
    cur = db_object.get_cursor()
    for col in col_list:
        query = """
            select
                to_char(
                    (count(
                        case 
                            when replace({0},' ','') = '' then 1		
                        end
                    )::float/
                    count({0})::float * 100
                )::numeric, 'FM999999999.00')
            from us_accidents;
        """.format(col)
        cur.execute(query)
        pct_val = cur.fetchone()[0]
        if pct_val != '.00':
            print("{0}: {1}%".format(col, pct_val))
    db_object.close()

def count_distinct_values():
    col_list = get_column_names()
    db_object = DBConnect()
    cur = db_object.get_cursor()
    dict_pct_missing = {}
    for col in col_list:
        query = """
            select 
                count(distinct ({0}))
            from us_accidents
        """.format(col)
        cur.execute(query)
        print("{0}: {1}".format(col, cur.fetchone()[0]))
    db_object.close()


def write_list_to_csv(dict_param,file_name):
    with open(file_name, 'w') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, dict_param.keys())
        w.writeheader()
        w.writerow(dict_param)

# def accident_count_per_year():
#     query = """
#         SELECT 
#             EXTRACT(YEAR FROM to_timestamp("start_time", 'YYYY-MM-DD'))::text as accident_year,
#             count(*) as accident_count
#         from us_accidents
#         group by 1 order by 1;
#     """

#     db_object = DBConnect()
#     conn = db_object.get_con()
#     df = pd.read_sql(query,conn)
#     db_object.close()

#     x = df['accident_year']
#     y = df['accident_count']

def plot():
    query = """
        select 
            severity,
            state
        from us_accidents limit 10;
    """

    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)
    db_object.close()

    y = df['state']#np.sin(x)
    x = df['severity']#np.linspace(0, 10, 30)
    
    plt.plot(x, y, 'o', color='black')
    plt.show()

def plot_bar_chart(query,x_label,y_label,title_label):
    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)
    db_object.close()

    x_axis = list(df[x_label])
    x_axis_len = len(x_axis)
    accident_count = list(df[y_label])
    ind = np.arange(x_axis_len)
    width = 0.35       

    
    pl = plt.subplots(figsize=(16,8))
    plt.bar(ind, accident_count, width)
    
    plt.ylabel('Accident Count')
    plt.title('Accident Count Per {0}'.format(title_label))    
    plt.xticks(ind, x_axis)

def severity_over_time_plot(query,fig_width=16,fig_height=8):
    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)
    db_object.close()
    
    year_list = list(df['year'].unique())
    severity_list = list(df['severity'].sort_values().unique())    
    plt.subplots(figsize=(fig_width,fig_height))
    for severity in severity_list:
        val_list = []
        for year in year_list:
            val = df.loc[(df['severity'] == severity) & (df['year']==year),'accident_count'].sum()
            val_list.append(val)
        plt.plot(year_list, val_list,label='severity {0}'.format(severity))        
    
    plt.legend()
    plt.suptitle('Accidents vs. Time/severity')
    
# def severity_over_time_plot(query,x_label,y_label,title_label,fig_width=16,fig_height=8):
#     db_object = DBConnect()
#     conn = db_object.get_con()
#     df = pd.read_sql(query,conn)
#     db_object.close()
    
#     year_list = list(df[x_label].unique())
#     severity_list = list(df[y_label].sort_values().unique())    
#     plt.subplots(figsize=(fig_width,fig_height))
#     for severity in severity_list:
#         val_list = []
#         for year in year_list:
#             val = df.loc[(df[y_lable] == severity) & (df[x_label]==year),'accident_count'].sum()
#             val_list.append(val)
#         plt.plot(year_list, val_list,label='{1} {0}'.format(severity))        
    
#     plt.legend()
#     plt.suptitle(title_label)

def render_streamlit_bar_chart(query,x_axis,y_axis,tooltip):
    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)
    container = alt.Chart(df).mark_bar().encode(x=alt.X(x_axis,sort=None), y=y_axis,tooltip=tooltip)
    st.altair_chart(container, use_container_width=True)

def render_streamlit_line_chart(query,x_axis,y_axis,metric):
    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)

    container = alt.Chart(df).mark_line().encode( 
            x=x_axis, 
            y=y_axis,
            color=metric,
            strokeDash=metric,
            tooltip=metric
            )
    st.altair_chart(container, use_container_width=True)

def accident_count_per_year():
    query = """
        SELECT 
            EXTRACT(YEAR FROM to_timestamp("start_time", 'YYYY-MM-DD'))::text as accident_year,
            count(*) as accident_count
        from us_accidents
        group by 1 order by 1;
    """

    render_streamlit_bar_chart(query,'accident_year','accident_count',['accident_count'])

def accident_count_per_tmc():
    query = """
        select
            case
                when tmc = '' then  'NULL'
                else tmc
            end as tmc,
            count(*) as accident_count
        from us_accidents
        group by 1 order by 2 desc;
    """
    
    render_streamlit_bar_chart(query,'tmc','accident_count',['accident_count'])

def accident_count_per_severity():
    query = """
        select
            severity,
            count(*) as accident_count
        from us_accidents
        group by 1 order by 2 desc;
    """
    
    render_streamlit_bar_chart(query,'severity','accident_count',['accident_count'])
    
def accident_count_per_state():
    query = """
        select
            state,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """

    render_streamlit_bar_chart(query,'state','accident_count',['accident_count'])


def accident_count_per_city():
    query = """
        select
            city,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """
    render_streamlit_bar_chart(query,'city','accident_count',['accident_count'])

def accident_count_per_zipcode():
    query = """
        select
            zipcode,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """
    render_streamlit_bar_chart(query,'zipcode','accident_count',['accident_count'])


def accident_count_per_visibility():
    query = """
        select
            visibilitymi as visibility,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """
    render_streamlit_bar_chart(query,'visibility','accident_count',['accident_count'])

def accident_count_per_weather_condition():
    query = """
        select
            weather_condition,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """
    render_streamlit_bar_chart(query,'weather_condition','accident_count',['accident_count'])

def accident_count_per_speed_bump():
    query = """
        select
            bump,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """
    render_streamlit_bar_chart(query,'bump','accident_count',['accident_count'])

def accident_count_per_windmph():
    query = """
        select
            wind_speedmph,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc
        limit 10;
    """
    render_streamlit_bar_chart(query,'wind_speedmph','accident_count',['accident_count'])

def accident_count_sunrise_sunset():
    query = """
        select
            case 
                when sunrise_sunset = '' then 'NULL'
                else sunrise_sunset
            end as sunrise_sunset,
            count(*) as accident_count
        from us_accidents
        group by 1
        order by 2 desc;
    """
    render_streamlit_bar_chart(query,'sunrise_sunset','accident_count',['accident_count'])

def severity_over_year():
    query="""
            select
                severity,
                EXTRACT(YEAR FROM to_timestamp(start_time, 'YYYY/MM/DD'))::text as "year",
                count(*) as accident_count
            from us_accidents
            group by 1,2 order by 2;
    """
    render_streamlit_line_chart(query,"year",'accident_count','severity')

def severity_over_quarter():
    query="""
            select
                severity,
                concat_ws(
                    '-',
                    EXTRACT(YEAR FROM to_timestamp(start_time, 'YYYY/MM/DD'))::text,
                    concat(
                        'Q',
                        EXTRACT(quarter FROM to_timestamp(start_time, 'YYYY/MM/DD'))::text
                    )	
                ) as time,
                count(*) as accident_count
            from us_accidents
            group by 1,2 order by 2,3,1;
    """
    render_streamlit_line_chart(query,"time",'accident_count','severity')


def severity_over_month():
    query="""
            select
                severity,
                to_char(date_trunc('month', to_timestamp(start_time, 'YYYY/MM/DD'))::date, 'YYYY-mm') as time,
                count(*) as accident_count
            from us_accidents
            group by severity,date_trunc('month', to_timestamp(start_time, 'YYYY/MM/DD'))
            ORDER BY date_trunc('month', to_timestamp(start_time, 'YYYY/MM/DD'));
    """
    render_streamlit_line_chart(query,"time",'accident_count','severity')

def weather_over_quarter():
    query="""
            select
                    weather_condition,
                    EXTRACT(YEAR FROM to_timestamp(start_time, 'YYYY/MM/DD'))::text as time,
                    count(*) as accident_count
                from us_accidents
                where 
                    weather_condition in (
                        select 
                            weather_condition
                            from (
                                select
                                    weather_condition,
                                    count(*) as accident_count
                                from us_accidents
                                group by 1
                                order by 2 desc
                            ) sub 
                        limit 10    		
                    )
                    and weather_condition <> ''
                group by 1,2 order by 2,3,1;
    """
    render_streamlit_line_chart(query,"time",'accident_count','weather_condition')

def get_state_list():
    query = """
        select
            distinct(state) as state
        from us_accidents
        where start_lng <> ''
        order by 1
    """#.format(severity)
    
    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)

    return list(df['state'])

def accident_map_locations(selected_state='TX',severity='1'):
    query = """
        select
            latitude,
            longitude
        from lat_long_by_state_severity
        where 
            state = '{0}'
            and severity = '{1}'
        group by 1,2;
    """.format(selected_state,severity)
    
    db_object = DBConnect()
    conn = db_object.get_con()
    df = pd.read_sql(query,conn)
    
    states = alt.topo_feature(data.us_10m.url, feature='states')

    #US states background
    background = alt.Chart(states).mark_geoshape(
        fill='lightgray',
        stroke='white'
    ).properties(
        width=1200,
        height=800
    ).project('albersUsa')

    points = alt.Chart(df).mark_point().encode(
        latitude='latitude',
        longitude='longitude'
    )

    background + points

def spearmans_rank_correlation():
    seed(1)
    data1 = rand(1000) * 20
    data2 = data1 + (rand(1000) * 10)
    pyplot.scatter(data1, data2)
    pyplot.show()


if __name__ == "__main__":
    st.text('Accident Count per State/Severity')
    state_list = get_state_list()

    selected_state = st.selectbox('Select State',state_list)
    selected_severity = st.selectbox('Select Severity',['1','2','3','4'])
    accident_map_locations(selected_state=selected_state,severity=selected_severity)

    st.text('The following displays the accident count per year from 2015-2020:')
    accident_count_per_year()

    st.text('The following displays the accident count per Traffic Message Control (TMC):')
    accident_count_per_tmc()

    st.text('The following displays the accident count per Severity:')
    accident_count_per_severity()

    st.text('The following displays the accident count per State (top 10):')
    accident_count_per_state()

    st.text('The following displays the accident count per City (top 10):')
    accident_count_per_city()

    st.text('The following displays the accident count per Zipcode (top 10):')
    accident_count_per_zipcode()

    st.text('The following displays the accident count per Visibility Type (top 10):')
    accident_count_per_visibility()

    st.text('The following displays the accident count per Weather Condition (top 10):')
    accident_count_per_weather_condition()

    st.text('The following displays the accident count per Speed Bump:')
    accident_count_per_speed_bump()

    st.text('The following displays the accident count per Wind Speed-mph:')
    accident_count_per_windmph()

    st.text('The following displays the accident count per Sunrise/Sunset:')
    accident_count_sunrise_sunset()

    st.text('Accident Count of Severity Per Year:')
    severity_over_year()

    st.text('Accident Count of Severity Per Year-Quarter :')
    severity_over_quarter()

    st.text('Accident Count of Severity Per Year-Month :')
    severity_over_month()

    st.text('Accident Count per year during certain weather conditions:')
    weather_over_quarter()

    