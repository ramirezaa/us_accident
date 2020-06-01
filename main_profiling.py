import pandas as pd
import os
from db_manager import *
import csv
import numpy as np
import altair as alt

import streamlit as st
import altair as alt
from vega_datasets import data
from scipy.stats import spearmanr,pearsonr
from sklearn.cluster import KMeans,MiniBatchKMeans
from sklearn.mixture import GaussianMixture

main_spearman_list = []
main_pearson_list = []

def spearmans_rank_correlation(source,x_col,y_col,ordinal_values = False,n_clusters=3,clustering_method='KMeans'):
    
    column_names = source.columns

    for name in column_names:
        source[name].fillna(0, inplace=True)

    st.markdown("**{0} vs. {1} **".format(x_col,y_col))
    container = alt.Chart(source).mark_point().encode(
        x=x_col,
        y=y_col
    )

    st.altair_chart(container, use_container_width=True)
    
    # variable_clustering(x_col,ordinal_values=ordinal_values)
    if not ordinal_values:
        variable_clustering(x_col,n_clusters=n_clusters,clustering_method=clustering_method)

    # calculate spearman's correlation
    coef, p = spearmanr(source[x_col], source[y_col])
    
    st.markdown('**Spearman Measure**')
    st.text("Correlation coefficient: {0}".format(coef))

    # interpret the significance
    alpha = 0.05
    if p > alpha:
        st.text('Samples are uncorrelated (fail to reject H0) p={0}'.format(p) )
    else:
        st.text('Samples are correlated (reject H0) p={0}'.format(p) )

    return coef,p

def spearmans_rank_correlation_solo(source,x_col,y_col):
    
    # calculate spearman's correlation
    coef, p = spearmanr(source[x_col], source[y_col])
    
    st.markdown('**Spearman Measure**')
    st.text("Correlation coefficient: {0}".format(coef))

    # interpret the significance
    alpha = 0.05
    if p > alpha:
        st.text('Samples are uncorrelated (fail to reject H0) p={0}'.format(p) )
    else:
        st.text('Samples are correlated (reject H0) p={0}'.format(p) )

    return coef,p

def pearson_rank_correlation(source,x_col,y_col):
    
    # column_names = source.columns

    # for name in column_names:
    #     source[name].fillna(0, inplace=True)

    # st.markdown("**{0} vs. {1} **".format(x_col,y_col))
    # container = alt.Chart(source).mark_point().encode(
    #     x=x_col,
    #     y=y_col
    # )

    # st.altair_chart(container, use_container_width=True)
    
    # calculate spearman's correlation
    coef, p = pearsonr(source[x_col], source[y_col])
    
    st.markdown('**Pearson Measure**')
    st.text("Correlation coefficient: {0}".format(coef))

    # interpret the significance
    alpha = 0.05
    if p > alpha:
        st.text('Samples are uncorrelated (fail to reject H0) p={0}'.format(p) )
    else:
        st.text('Samples are correlated (reject H0) p={0}'.format(p) )
    
    return coef, p


def get_data_source():
    return data.cars()

def get_us_accident_source(column_name, ordinal_values = False, limit = '', order='', where=''):
    db_object = DBConnect()
    conn = db_object.get_con()
    
    if not ordinal_values:
        query = """
            select 
                {0},
                sum(accident_count) as accident_count
            from us_accidents_min {1}            
            group by 1
            {2}
            {3};
        """.format(column_name, where, order, limit)
    else:
        query = """
            select
                {0},
                sum(accident_count) as accident_count
            from us_accidents_min
            group by 1;
        """.format(column_name)

    df = pd.read_sql(query,conn)

    return df

def set_spearman_process(columns,n_clusters=3,clustering_method='KMeans',ordinal_values=False):
    
    x_col = "accident_count"
    
    for y_col in columns:
        # print('current col: {0}'.format(y_col))
        source = get_us_accident_source(y_col,ordinal_values=ordinal_values)
        spearmans_rank_correlation(source,y_col,x_col,ordinal_values=ordinal_values,n_clusters=n_clusters,clustering_method=clustering_method)
        pearson_rank_correlation(source,y_col,x_col)
        # coef,p = spearmans_rank_correlation(source,y_col,x_col,ordinal_values=ordinal_values,n_clusters=n_clusters,clustering_method=clustering_method)
        # return_dict = {"column_name":y_col,"coefficient":coef,"p":p}
        # main_spearman_list.append(return_dict)

def set_bar_chart_process(columns,ordinal_values=False):
    
    x_col = "accident_count"
    
    for obj in columns:
        y_col = obj['col']
        limit = obj['limit']
        order = obj['order']
        where = obj['where']
        is_numeric = obj['is_numeric']
        st.text(obj['title'])
        source = get_us_accident_source(y_col,ordinal_values=ordinal_values,limit=limit,order=order,where=where)
        render_streamlit_bar_chart(source,y_col,x_col,[x_col])
        
        if is_numeric:
            spearmans_rank_correlation_solo(source,y_col,x_col)
            pearson_rank_correlation(source,y_col,x_col)

def set_pearson_process(columns,ordinal_values=False):
    x_col = "accident_count"
    for y_col in columns:
        source = get_us_accident_source(y_col,ordinal_values=ordinal_values)
        coef,p = pearson_rank_correlation(source,y_col,x_col)
        return_dict = {"column_name":y_col,"coefficient":coef,"p":p}
        main_pearson_list.append(return_dict)

def get_clustering_source(column_name,ordinal_values=False):
    # print("get_clustering_source: {0}".format(ordinal_values ) )
    if not ordinal_values:
        query = """
                select
                    {0},
                    sum(accident_count) as accident_count
                from us_accidents_min
                group by 1;
        """.format(column_name)
    else:
        query = """
                select
                    {0},
                    sum(accident_count) as accident_count
                from us_accidents_min
                where 
                    {0} <> ''
                group by 1;
        """.format(column_name)


    db_object = DBConnect()
    conn = db_object.get_con()

    df = pd.read_sql(query,conn)

    return df

def variable_clustering(column_name,ordinal_values=False,n_clusters=3,clustering_method='KMeans'):

    df = get_clustering_source(column_name,ordinal_values=ordinal_values)

    cluster_method = clustering_method.replace(' ','').lower()

    clustered_data = None
    if cluster_method == 'kmeans':
        print('k means')
        clustered_data = KMeans(n_clusters=n_clusters)

    elif cluster_method == 'kmeansminibatch':
        print('k means minibatch')
        clustered_data = MiniBatchKMeans(n_clusters=n_clusters,random_state=0,batch_size=6)
    # elif cluster_method == 'gaussianmixture':
    #     print('gaussian mixture')
    #     clustered_data = GaussianMixture(n_components=n_clusters)
    
    clustered_data.fit(df)
    df['labels'] = clustered_data.labels_

    container = alt.Chart(df).mark_point().encode(
        x=column_name,
        y="accident_count",
        color='labels:N'
    )

    st.altair_chart(container, use_container_width=True)

def get_column_names():

    query = """
        SELECT 
            distinct(column_name) as column_names
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name   = 'us_accidents_min';
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
            from us_accidents_min;
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
            from us_accidents_min
        """.format(col)
        cur.execute(query)
        print("{0}: {1}".format(col, cur.fetchone()[0]))
    db_object.close()


def write_list_to_csv(dict_param,file_name):
    with open(file_name, 'w') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, dict_param.keys())
        w.writeheader()
        w.writerow(dict_param)

def render_streamlit_bar_chart(df,x_axis,y_axis,tooltip):
    db_object = DBConnect()
    conn = db_object.get_con()
    container = alt.Chart(df).mark_bar().encode(
            x=alt.X(x_axis+":O",sort=None), 
            y=y_axis,tooltip=tooltip
        )
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
                    sum(accident_count) as accident_count
                from us_accidents_min
                where 
                    weather_condition in (
                        select 
                            weather_condition
                            from (
                                select
                                    weather_condition,
                                    sum(accident_count) as accident_count
                                from us_accidents_min
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

@st.cache
def get_accident_map_locations_df(selected_state='TX',severity='1'):
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

    return df

def accident_map_locations(selected_state='TX',severity='1'):
    df = get_accident_map_locations_df(selected_state=selected_state,severity=severity)
    
    states = alt.topo_feature(data.us_10m.url, feature='states')

    #US states background
    background = alt.Chart(states).mark_geoshape(
        fill='lightgray',
        stroke='white'
    ).properties(
        width=1000,
        height=800
    ).project('albersUsa')

    points = alt.Chart(df).mark_point().encode(
        latitude='latitude',
        longitude='longitude'
    )

    background + points

if __name__ == "__main__":
    bar_chart_cols = [  
                        {'col': 'year', 'limit':'', 'order':'', 'where':'','title':'','is_numeric':False},
                        {'col': 'tmc', 'limit':'', 'order':'', 'where':'','title':'Note: \n TMC = Traffic Message Channel (a code poviding a more detailed description of the event)','is_numeric':False},
                        {'col': 'severity', 'limit':'', 'order':'', 'where':'','title':'Note: \n1 indicates least impact on traffic, \n4 indicates a significant impact on traffic i.e. long delay','is_numeric':False},
                        # {'col': 'state', 'limit':'', 'order':'', 'where':'','title':'','is_numeric':False},
                        {'col': 'state', 'limit':'limit 10', 'order':'order by 2 desc', 'where':'','title':'','is_numeric':False},
                        {'col': 'city', 'limit': 'limit 10','order': 'order by 2 desc','where':'','title':'','is_numeric':False},
                        {'col': 'zipcode','where':'', 'order': 'order by 2 desc', 'limit': 'limit 10','title':'','is_numeric':False},
                        # {'col': 'visibilitymi', 'order': 'order by 1 asc', 'limit':'', 'where':'','title':'','is_numeric':True},
                        {'col': 'visibilitymi', 'order': 'order by 1 asc','where': 'where visibilitymi < 14 and visibilitymi >3','limit':'','title':'','is_numeric':True},
                        # {'col': 'weather_condition', 'limit':'', 'order':'', 'where':'','title':'','is_numeric':False},
                        {'col': 'weather_condition', 'limit':'limit 10', 'order':'order by 2 desc', 'where':'','title':'','is_numeric':False},
                        {'col': 'bump', 'limit':'', 'order':'', 'where':'','title':'','is_numeric':False},
                        {'col': 'wind_speedmph', 'limit':'limit 10', 'order':'order by 2 desc', 'where':'','title':'','is_numeric':True},
                        {'col': 'sunrise_sunset', 'limit':'', 'order':'', 'where':'','title':'','is_numeric':False},
                        # {'col': 'distancemi', 'limit':'limit 20', 'order':'order by 1 asc', 'where':'','title':'Note: The length of the road extent extent affected by the accident','is_numeric':True},
                        {'col': 'wind_chillf', 'limit':'', 'order':'order by 1 asc', 'where':' where wind_chillf::money::numeric > -1 and wind_chillf::money::numeric < 1','title':'','is_numeric':True},
                        # {'col': 'precipitationin', 'limit':'', 'order':'', 'where':'','title':'','is_numeric':True},
                        {'col': 'precipitationin', 'limit':'', 'order':'order by 1 asc', 'where': ' where precipitationin::money::numeric < 0.16','title':'','is_numeric':True}
                    ]

    add_clustering_method = st.sidebar.selectbox(
        'Clustering Method',
        ('KMeans', 'KMeansMiniBatch')
    )

    # Add a slider to the sidebar:
    add_slider_clusters = st.sidebar.slider(
        'Select a range of values',
        0, 10,(3)
    )
    ## columns = ['severity','distancemi','temperaturef','wind_chillf','humidity','pressurein','visibilitymi','wind_speedmph','precipitationin']
    
    st.text('Accident Count per State/Severity')
    state_list = get_state_list()

    selected_state = st.sidebar.selectbox('Select State',state_list)
    selected_severity = st.sidebar.selectbox('Select Severity',['1','2','3','4'])
    accident_map_locations(selected_state=selected_state,severity=selected_severity)

    # st.text('The following displays the accident count per year from 2015-2020:')
    set_bar_chart_process(bar_chart_cols)

    st.text('Accident Count of Severity Per Year:')
    severity_over_year()

    st.text('Accident Count of Severity Per Year-Quarter :')
    severity_over_quarter()

    st.text('Accident Count of Severity Per Year-Month :')
    severity_over_month()

    st.text('Accident Count per year during certain weather conditions:')
    weather_over_quarter()

    columns = ['temperaturef','humidity','pressurein']
    # # st.title('SPEARMAN ANALYSIS')
    set_spearman_process(columns,n_clusters=add_slider_clusters,clustering_method=add_clustering_method)