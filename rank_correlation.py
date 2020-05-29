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
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
main_spearman_list = []
main_pearson_list = []

def spearmans_rank_correlation(source,x_col,y_col,ordinal_values = False):
    
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
        variable_clustering(x_col)

    # calculate spearman's correlation
    coef, p = spearmanr(source[x_col], source[y_col])
    
    st.text("Spearmans correlation coefficient: {0}".format(coef))

    # interpret the significance
    alpha = 0.05
    if p > alpha:
        st.text('Samples are uncorrelated (fail to reject H0) p={0}'.format(p) )
    else:
        st.text('Samples are correlated (reject H0) p={0}'.format(p) )

    return coef,p

def pearson_rank_correlation(source,x_col,y_col):
    
    column_names = source.columns

    for name in column_names:
        source[name].fillna(0, inplace=True)

    st.markdown("**{0} vs. {1} **".format(x_col,y_col))
    container = alt.Chart(source).mark_point().encode(
        x=x_col,
        y=y_col
    )

    st.altair_chart(container, use_container_width=True)
    
    # calculate spearman's correlation
    coef, p = pearsonr(source[x_col], source[y_col])
    
    st.text("Pearson correlation coefficient: {0}".format(coef))

    # interpret the significance
    alpha = 0.05
    if p > alpha:
        st.text('Samples are uncorrelated (fail to reject H0) p={0}'.format(p) )
    else:
        st.text('Samples are correlated (reject H0) p={0}'.format(p) )
    
    return coef, p


def get_data_source():
    return data.cars()

def get_us_accident_source(column_name, ordinal_values = False):
    db_object = DBConnect()
    conn = db_object.get_con()
    
    if not ordinal_values:
        query = """
            select 
                {0}::money::numeric,
                count(*) as accident_count
            from us_accidents
                where replace({0},' ','') <> ''
            group by 1;
        """.format(column_name)
    else:
        query = """
            select
                {0},
                count(*) as accident_count
            from us_accidents
            group by 1;
        """.format(column_name)

    df = pd.read_sql(query,conn)

    return df

def set_spearman_process(columns,ordinal_values=False):
    
    x_col = "accident_count"
    
    for y_col in columns:
        source = get_us_accident_source(y_col,ordinal_values=ordinal_values)
        coef,p = spearmans_rank_correlation(source,y_col,x_col,ordinal_values=ordinal_values)
        return_dict = {"column_name":y_col,"coefficient":coef,"p":p}
        main_spearman_list.append(return_dict)

def set_pearson_process(columns,ordinal_values=False):
    x_col = "accident_count"
    for y_col in columns:
        source = get_us_accident_source(y_col,ordinal_values=ordinal_values)
        coef,p = pearson_rank_correlation(source,y_col,x_col)
        return_dict = {"column_name":y_col,"coefficient":coef,"p":p}
        main_pearson_list.append(return_dict)

def get_clustering_source(column_name,ordinal_values=False):
    print("get_clustering_source: {0}".format(ordinal_values ) )
    if not ordinal_values:
        query = """
                select
                    {0}::money::numeric,
                    count(*) as accident_count
                from us_accidents
                where 
                    {0} <> ''
                group by 1;
        """.format(column_name)
    else:
        query = """
                select
                    {0},
                    count(*) as accident_count
                from us_accidents
                where 
                    {0} <> ''
                group by 1;
        """.format(column_name)


    db_object = DBConnect()
    conn = db_object.get_con()

    df = pd.read_sql(query,conn)

    return df

def variable_clustering(column_name,ordinal_values=False):

    df = get_clustering_source(column_name,ordinal_values=ordinal_values)
    kmeans = KMeans(n_clusters=3)
    kmeans.fit(df)
    df['labels'] = kmeans.labels_

    st.markdown("** K-means clustering K=3: {0} vs. {1} **".format(column_name,'accident_count'))
    container = alt.Chart(df).mark_point().encode(
        x=column_name,
        y="accident_count",
        color='labels:N'
    )

    st.altair_chart(container, use_container_width=True)

if __name__ == "__main__":
    #spearman rank correlation
    columns = ['severity','distancemi','temperaturef','wind_chillf','humidity','pressurein','visibilitymi','wind_speedmph','precipitationin']
    st.title('SPEARMAN ANALYSIS')
    set_spearman_process(columns)
    
    ordinal_cols = ['wind_direction','side','weather_condition','crossing']
    set_spearman_process(ordinal_cols,ordinal_values=True)

    #pearson rank correlation
    st.title('PEARSON ANALYSIS')
    set_pearson_process(columns)


    spearman_df = pd.DataFrame(main_spearman_list)    
    st.header("Final Spearman Rank Correlation Table")
    st.write(spearman_df)

    pearson_df = pd.DataFrame(main_pearson_list)
    st.header("Final Pearson Rank Correlation Table")
    st.write(pearson_df)

    