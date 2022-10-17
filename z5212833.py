from cProfile import label
import json
from statistics import mean
from unicodedata import name
from matplotlib import markers
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math
import re

studentid = os.path.basename(sys.modules[__name__].__file__)


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(10).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)

        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())


def question_1(routes, suburbs):
    """
    :param routes: the path for the routes dataset
    :param suburbs: the path for the routes suburbs
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    df1 = pd.read_csv(routes)
    df1['service_direction_name'] = df1['service_direction_name'].str.replace(r"\(.*\)","",regex=True)
    df1['start'] = df1['service_direction_name'].str.split(',').str.get(0)
    df1['start'] = df1['start'].str.split(' to').str.get(0)
    df1['start'] = df1['start'].str.split(' or ').str.get(0)
    df1['start'] = df1['start'].str.split(' and').str.get(0)
    df1['start'] = df1['start'].str.split(' Station').str.get(0)
    df1['end'] = df1['service_direction_name'].str.split(',').str.get(-1)
    df1['end'] = df1['end'].str.split(' to ').str.get(-1)
    df1['end'] = df1['end'].str.split('then ').str.get(-1)
    df1['end'] = df1['end'].str.split(' via').str.get(0)
    df1['end'] = df1['end'].str.split(' or ').str.get(0)
    df1['end'] = df1['end'].str.split('and ').str.get(-1)
    df1['end'] = df1['end'].str.split(' Station').str.get(0)
    log("QUESTION 1", output_df=df1[["service_direction_name", "start", "end"]], other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: dataframe df2
            Please read the assignment specs to know how to create the output dataframe
    """
    dfcombine = pd.concat([df1['start'],df1['end']],ignore_index=True)
    df2 = dfcombine.value_counts().rename_axis('service_location').reset_index(name='frequency')
    #################################################
    # Your code goes here ...
    #################################################
    df2 = df2.head(10)
    log("QUESTION 2", output_df=df2, other=df2.shape)
    return df2


def question_3(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    # Your code goes here ...
    #################################################
    df1['transport_name'] = df1['transport_name'].map(change)
    #print(df3.value_counts())
    df3 = df1
    log("QUESTION 3", output_df=df3[['transport_name']], other=df3.shape)
    return df3
def change(x):
    b = re.findall(r"bus", x, re.I)
    if len(b) != 0:
        #print (len(m))
        return 'Bus'
    t = re.findall(r"train", x, re.I)
    if len(t) != 0:
        return 'Train'
    lr = re.findall(r"light rail", x, re.I)
    if len(lr) != 0:
        return 'Light Rail'
    fr = re.findall(r"ferr", x, re.I)
    if len(fr) != 0:
        return 'Ferry'
    mt = re.findall(r"metro", x, re.I)
    if len(mt) != 0:
        return 'Metro'
    else:
        return 'Bus'
def question_4(df3):
    """
    :param df3: the dataframe created in question 3
    :param continents: the path for the Countries-Continents.csv file
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    
    #################################################
    # Your code goes here ...
    #################################################
    df4 = df3['transport_name'].value_counts(ascending = True).rename_axis('transport_name').reset_index(name='frequency')

    log("QUESTION 4", output_df=df4[["transport_name", "frequency"]], other=df4.shape)
    return df4


def question_5(df3, suburbs):
    """
    :param df3: the dataframe created in question 2
    :param suburbs : the path to dataset
    :return: df5
            Data Type: dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    # Your code goes here ...
    #################################################
    df3['depot_name'] = df3['depot_name'].str.split(', ').str.get(-1)
    df3 = df3.rename(columns={"depot_name": "suburb"})
    suburbs = pd.read_csv(suburbs)
    suburbs = suburbs.loc[suburbs['state'] == 'NSW']
    suburbs = suburbs.loc[suburbs['population'] != 0]
    pop = suburbs[['suburb', 'population', 'state']]
    df5 = df3[['suburb']]
    df5 = df5.value_counts().rename_axis('suburb').reset_index(name='ratio')
    df5 = pop.merge(df5, how='inner', on='suburb')
    df5['ratio'] = df5['ratio']/df5['population']
    df5 = df5.rename(columns={"suburb": "depot"})
    df5 = df5.sort_values(by='ratio',ascending=False)
    df5 = df5[['depot', 'ratio']]
    df5.set_index('depot', inplace=True)
    log("QUESTION 5", output_df=df5[["ratio"]], other=df5.shape)
    return df5


def question_6(df3):
    """
    :param df3: the dataframe created in question 3
    :return: nothing, but saves the figure on the disk
    """
    table = None
    df6 = df3[['operator_name','transport_name']]
    gp = df6.groupby(['operator_name','transport_name'])
    pd.set_option('display.max_rows',50)
    df6 = gp.size().to_frame('number')
    table = pd.pivot_table(df6, index=['operator_name', 'transport_name'], values=['number']).sort_values(by='number' ,ascending=False)
    #################################################
    # Your code goes here ...
    #################################################

    log("QUESTION 6", output_df=None, other=table)
    return table

def question_7(df3,suburbs):
    """
    :param df3: the dataframe created in question 3
    :param suburbs : the path to dataset
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################
    suburbs = pd.read_csv(suburbs)
    suburbs = suburbs.loc[suburbs['statistic_area'] == 'Greater Sydney']
    suburbs = suburbs.loc[suburbs['population'] != 0]
    suburbs['total_income'] = suburbs['median_income']*suburbs['population']
    df7 = suburbs[['total_income','population','local_goverment_area','sqkm']]
    gp = df7.groupby(['local_goverment_area']).sum()
    gp['current_median'] = gp['total_income']/gp['population']
    df7 = gp[['population','current_median','sqkm']]
    df7 = df7.rename(columns={"current_median": "median_income"})
    df7 = df7.sort_values(by = ['sqkm'],ascending=False)
    df7.reset_index(inplace=True)
    df7['local_goverment_area'] = df7['local_goverment_area'].str.replace(r"\(.*\)","",regex=True)
    df7 = df7.sort_values(by = ['sqkm'],ascending=True)
    l1, l2, l3, l4 = np.array_split(df7,4)
    ax = l1.plot('population','median_income',label= 'sqkm < 15', marker='o',markersize = 4, linestyle = 'None')
    l2.plot('population','median_income',label= 'sqkm < 45',ax = ax,marker='o',markersize = 6.5,linestyle = 'None')
    l3.plot('population','median_income',label= 'sqkm < 280',ax = ax,marker='o',markersize = 9.5,linestyle = 'None')
    l4.plot('population','median_income',label= 'sqkm > 280',ax = ax,marker='o',markersize = 10.5,linestyle = 'None')
    for a, b,c in zip(df7['population'],df7['median_income'],df7['local_goverment_area']):
        if c == 'Botany Bay ':
            continue
        if c == 'Hawkesbury ':
            continue
        else:
            ax.text(a,b,c, fontsize=6)
    plt.ylabel('median_income')
    plt.title('LGA comparison')
    plt.savefig("{}-Q7.png".format(studentid))


def question_8(df3,suburbs):
    """
    :param df3: the dataframe created in question 3
    :param suburbs : the path to dataset
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################
    suburbs = pd.read_csv(suburbs)
    suburbs = suburbs.loc[suburbs['state'] == 'NSW']
    suburbs = suburbs[['suburb','sqkm','lat','lng']]
    #print(suburbs)
    df8 = df3.loc[df3['transport_name'] != 'Bus']
    dfcombine = pd.concat([df8['start'],df8['end']],ignore_index=True)
    dffreq = dfcombine.value_counts().rename_axis('suburb').reset_index(name='frequency')
    dffreq = suburbs.merge(dffreq, how='inner', on='suburb')
    #print(df8.head(20))
    dffreq = dffreq.sort_values(by='frequency',ascending=False)
    dffreq = dffreq.head(25)
    #print(dffreq)
    df8 = df8.rename(columns={"start": "suburb"})
    dfroute = df8.merge(dffreq, how='inner', on = 'suburb')
    dfroute = dfroute.rename(columns={"suburb": "start"})
    dfroute = dfroute.rename(columns={"end": "suburb"})
    dfroute = dfroute.merge(dffreq, how='inner', on = 'suburb')
    dfroute = dfroute.rename(columns={"suburb": "end"})
    dfroute = dfroute[['start','end','sqkm_x','sqkm_y','lat_x','lat_y','lng_x','lng_y','transport_name']]
    dffreq = dffreq.sort_values(by='sqkm',ascending=True)
    l1, l2, l3 = np.array_split(dffreq,3)
    #print(l3)
    plt.figure(figsize=(15,3))
    ax = l1.plot('lng','lat',label= 'sqkm < 20', marker='o',markersize = 6, linestyle = 'None')
    l2.plot('lng','lat',label= '20 < sqkm < 150',ax = ax,marker='o',markersize = 9,linestyle = 'None')
    l3.plot('lng','lat',label= 'sqkm > 150',ax = ax,marker='o',markersize = 12,linestyle = 'None')
    plt.plot((dfroute['lng_x'],dfroute['lng_y']),(dfroute['lat_x'],dfroute['lat_y']),color='orange',label='Train')
    for a, b,c in zip(dfroute['lng_x'],dfroute['lat_x'],dfroute['start']):
        if c == 'Manly':
            continue
        elif c == 'Wee Waa':
            ax.text(a,b,c,ha='right',fontsize=7)
        elif c == 'Moss Vale':
            ax.text(a,b,c,ha='right',fontsize=7)
        elif c == 'Bathurst':
            ax.text(a,b,c,ha='right',fontsize=7)
        else:
            ax.text(a,b,c, fontsize=7)
    plt.ylabel('latitude')
    plt.xlabel('longitude')
    plt.title('NSW Train Services between 25 frequent visited suburbs')
    plt.savefig("{}-Q8.png".format(studentid))



if __name__ == "__main__":
    df1 = question_1("routes.csv", "suburbs.csv")
    df2 = question_2(df1.copy(True))
    df3 = question_3(df1.copy(True))
    df4 = question_4(df3.copy(True))
    df5 = question_5(df3.copy(True), "suburbs.csv")
    table = question_6(df3.copy(True))
    question_7(df3.copy(True), "suburbs.csv")
    question_8(df3.copy(True), "suburbs.csv")