# -*- coding: utf-8 -*-
"""
Created on Sun Jan 08 2023

@author: Elvis Crypto
"""


import streamlit as st
import pandas as pd
from pandasql import sqldf
import plost
from datetime import datetime
import altair as alt


st.set_page_config(layout='wide', initial_sidebar_state='expanded')

# Sidebar stuff
st.sidebar.header('DeFi Perpetuals Dashboard ')
st.sidebar.subheader('Select protocol to explore.')
selected_protocol= st.sidebar.radio('Protocol Name',['GMX','dYdX','Perpetual Protocol','ApolloX'],index=0)
show_sanity = st.sidebar.checkbox('Show sanity check graphs',True)

protocol2Loader = {
    'GMX':'gmx.csv',
    'dYdX':'dydx.csv',
    'Perpetual Protocol':'perpetual-protocol.csv',
    'ApolloX':'apollox.csv'
    } 
st.sidebar.markdown('''
---
Created with ❤️ by [@Elv1s_Crypto](https://twitter.com/Elv1s_Crypto/).
---
Data📊 by [DefiLlama](https://defillama.com/).
''')

# Correctional measures on the data
def correctLlama(df):
    def copyHeader(df_header, idx):
        header_temp = df_header.iloc[:,idx]
        return header_temp
    
    corr = df.copy()
    protocol = df.columns[1]
    df_body = df.iloc[5:,1:].copy()
    df_header = df.iloc[0:4,:].copy()
    df_body = df_body.astype(float)
    df_body.insert(0,'dummy',0)
    df_body = df_body.fillna(0)
    if(protocol=='Perpetual Protocol'):
        # Total staking is ethereum-staking but more complete
        corr.iloc[5:,2] = corr.iloc[5:,4]
        corr.iloc[5:,6] = corr.iloc[5:,8]
        corr.iloc[5:,10] = corr.iloc[5:,12]
        # Should be TVL on ETH not in staking (in USDC)
        offs=0
        value = df_body.iloc[:,3] - df_body.iloc[:,1]
        header = copyHeader(df_header, 3)
        header[1] = 'ethereum'
        value_ins = pd.concat([header,value], axis=0)
        corr.insert(2+offs,'Inserted1',value_ins)
        offs+=1
        
        header = copyHeader(df_header, 7)
        header[1] = 'ethereum'
        value_ins = pd.concat([header,value], axis=0)
        corr.insert(6+offs,'Inserted2',value_ins)
        offs+=1
        
        header = copyHeader(df_header, 11)
        header[1] = 'ethereum'
        value = df_body.iloc[:,11] - df_body.iloc[:,9]
        value_ins = pd.concat([header,value], axis=0)
        corr.insert(10+offs,'Inserted3',value_ins)
        offs+=1
    elif(protocol=='ApolloX'):
        # This is mostly fine except for some unncessary duplicates cols
        corr.drop(columns=corr.columns[[3,6,13,14,29,30]],inplace=True)
    elif(protocol=='dYdX'):
        # total same as ethereum where exists
        corr.iloc[5:,1] = corr.iloc[5:,2]
    return corr

# Read the data
def readLlama(url):
#    pysqldf = lambda q: sqldf(q, globals())
    
#    df = pd.read_csv(url, parse_dates=['Date'],dayfirst=True)
    df = pd.read_csv(url)
    df.drop(['Unnamed: 0','Timestamp'], axis=1, inplace = True)
    df = correctLlama(df)
#    df['Date'] = pd.to_datetime(df['Date'],format='%Y-%m-%d %H:%M:%S.%f')
#    df['Date'] = pd.to_datetime(df['Date'],format='%d/%m/%Y')
    header = df.iloc[0:4,1:]
    body = df.iloc[4:,1:]
    body = body.astype(float).round(1)
    dates = df.iloc[4:,:1]
    dates['Date'] = dates['Date'].apply(lambda x: datetime.strptime(x,'%d/%m/%Y').date())
    new_set = []
    Llama_set =[]
    for column_name in header.iloc[2,:].unique():
        # Get columns indexes column name applies
        selected_cols = header.iloc[2,:] == column_name
        subheader = header.loc[:,selected_cols]
        subbody = body.loc[:,selected_cols]
        new_tall = pd.DataFrame(columns=['Date',column_name])
        new_Llama = pd.DataFrame(columns=['Date',column_name])
        # Stack all existing combos of 1,3 row
        for origin_idx in range(len(subheader.iloc[1,:])):
            origin = subheader.iloc[1,origin_idx]
            currency = subheader.iloc[3,origin_idx]
            new_df = pd.DataFrame(data=pd.concat([dates,subbody.iloc[:,origin_idx]], axis=1).rename(columns={subbody.columns[origin_idx]:column_name}))
            new_df['Currency'] = currency
            #if '-' not in origin:
            if 'Total' != origin: # This is not a Llama total
                new_df['Origin'] = origin
                new_tall = pd.concat([new_tall,new_df],ignore_index=True)
                new_tall['Category'] = header.iloc[0,0]
                new_tall['Protocol'] = header.columns[0]
            else: # This is a Llama Total
                new_Llama = pd.concat([new_Llama,new_df],ignore_index=True)
                new_Llama['Category'] = header.iloc[0,0]
                new_Llama['Protocol'] = header.columns[0]
        new_set.append(new_tall)
        Llama_set.append(new_Llama)
    q1 = """ SELECT *
              FROM df3 NATURAL LEFT OUTER JOIN df2
              """
    df1 = new_set[0]
    df1.drop('Currency', axis=1,inplace=True)
    df2 = new_set[1]
    df3 = new_set[2]
    TVL_all = sqldf(q1)
    TVL_origin = df1
    q2 = """ SELECT Date, sum(TVL) as TVL
              FROM df1
                WHERE Origin <> 'staking'
              GROUP BY Date
              """
    TVL_Total = sqldf(q2)
    Llama_TVL_Total = Llama_set[0]
    Llama_TVL_Total.drop('Currency', axis=1,inplace=True)
    df2 = Llama_set[1]
    df3 = Llama_set[2]
    Llama_TVL_Currency = sqldf(q1)
    Llama_TVL_Currency['Category'] = header.iloc[0,0]
    Llama_TVL_Currency['Protocol'] = header.columns[0]
    return TVL_all, TVL_origin, TVL_Total, Llama_TVL_Currency, Llama_TVL_Total
    

#Apollo_all, Apollo_origin, Apollo_Total, Apollo_Llama_TVL_Currency, Apollo_Llama_TVL_Total = readLlama('apollox.csv')
#dydx_all, dydx_origin, dydx_Total, dydx_Llama_TVL_Currency, dydx_Llama_TVL_Total = readLlama('dydx.csv')
#gmx_all, gmx_origin, gmx_Total, gmx_Llama_TVL_Currency, gmx_Llama_TVL_Total = readLlama('gmx.csv')
#perp_all, perp_origin, perp_Total, perp_Llama_TVL_Currency, perp_Llama_TVL_Total = readLlama('perpetual-protocol.csv')

current_test=protocol2Loader[selected_protocol]
# current_test='perpetual-protocol.csv'
test_all, test_origin, test_Total, test_Llama_TVL_Currency, test_Llama_TVL_Total = readLlama(current_test)

q1 = """ SELECT T.Date, L.Category, L.Protocol, T.TVL as 'TVL_summed', L.TVL as 'TVL_Llama'
          FROM test_Total T LEFT OUTER JOIN test_Llama_TVL_Total L USING (Date)
          """

test_both = sqldf(q1)
st.markdown(f'''
Dataset Overview {test_both['Protocol'][0]}
---
''')


q2 = """ SELECT T.Date, L.Category, L.Protocol, T.TVL*0.98 as 'TVL_noStaking_shifted2prtc', L.TVL as 'TVL_Llama'
          FROM (
            SELECT Date, Category, Protocol, sum(TVL) as TVL
              FROM test_origin 
                WHERE Origin not like '%staking%'
              GROUP BY 1,2,3
          ) T          
          LEFT OUTER JOIN test_Llama_TVL_Total L USING (Date)
          """
test_bothNoStake = sqldf(q2)

if(show_sanity):
    st.markdown(f'''
    #### Summed TVL Vs. DeFi Llama TVL for {test_both['Protocol'][0]}
    ''')
    plost.line_chart(
        test_both,
        x='Date',
        y=['TVL_summed','TVL_Llama'],
        height=600,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )
    
    st.markdown(f'''
    ---
    #### Is DeFi Llama Total == Sum(Other origins) without staking?
    ''')
    plost.line_chart(
        test_bothNoStake,
        x='Date',
        y=['TVL_noStaking_shifted2prtc','TVL_Llama'],
        height=600,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )

c1, c2 = st.columns((7,3))
with c1:
    st.markdown(f'''
    ---
    #### Summed TVL Composition
    ''')
    plost.area_chart(
        test_origin[test_origin['Origin']!='staking'],
        x='Date',
        y='TVL',
        color='Origin',
        stack=True,
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )

q3 = """ SELECT Origin, AVG(TVL) as TVL
           FROM test_origin, (SELECT max(Date) as maxdate FROM test_origin)
             WHERE Date > date(maxdate, '-7 day')
               AND Origin <> 'staking'
           GROUP BY 1
          """
test_Origin_lastTVL = sqldf(q3)

with c2:
    st.markdown(f'''
    ---
    #### Current Composition
    ''')
    plost.donut_chart(
        test_Origin_lastTVL,
        theta='TVL',
        color='Origin'
        )

# Superimpose stacked bargraph with actual TVL
q4 = """ SELECT wk, Origin, TVL - LAG(TVL) OVER (PARTITION BY Origin ORDER BY wk) AS TVL_change
          FROM (
            SELECT date(strftime('%s', Date) - 86400 * ((strftime('%w', Date) + 6) % 7), 'unixepoch') as wk, Origin, AVG(TVL) as TVL
              FROM test_origin
                WHERE Origin <> 'staking'
              GROUP BY 1,2
            )
          """
test_Origin_wOw = sqldf(q4)
q4p1 = """ SELECT T.*, W.*
             FROM test_Total T LEFT JOIN test_Origin_wOw W ON T.Date = W.wk

"""
test_Origin_wOw_expanded = sqldf(q4p1)
base = alt.Chart(test_Origin_wOw_expanded).encode(
    alt.X('Date:T', axis=alt.Axis(title=None))
)


bar = base.mark_bar().encode(
    alt.Y('sum(TVL_change):Q',axis=alt.Axis(title='TVL Change ($)')),
    color='Origin'
).interactive()
line = base.mark_line().encode(
    alt.Y('TVL:Q',axis=alt.Axis(title='TVL'))
).interactive()
# c=alt.layer(line,bar).resolve_scale(
c=alt.layer(bar,line).resolve_scale(
    y='independent'
    )
c3, c4 = st.columns((5,5))
with c3:
    st.markdown(f'''
    #### Week over week change of TVL by origin 
    ''')
    st.altair_chart(c, use_container_width=True)
with c4:
    st.markdown(f'''
    #### %Wise composition of TVL by origin 
    ''')
    plost.area_chart(
        test_origin[test_origin['Origin']!='staking'],
        x='Date:T',
        y='TVL:Q',
        color='Origin',
        stack='normalize',
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )

st.markdown(f'''
---
#### TVL Breakdown by currency
''')

q5 = """ SELECT Date, Currency, sum("Tokens(USD)") as TVL
           FROM test_all
             WHERE Origin <> 'staking'
           GROUP BY 1,2
          """
test_TVL_currency = sqldf(q5)
c1, c2 = st.columns((7,3))
with c1:
    st.markdown(f'''
    ---
    #### Summed TVL Composition by Currency
    ''')
    plost.area_chart(
        test_TVL_currency,
        x='Date',
        y='TVL',
        color='Currency',
        stack=True,
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )

q6 = """ SELECT Currency, AVG("Tokens(USD)") as TVL
           FROM test_all, (SELECT max(Date) as maxdate FROM test_origin)
             WHERE Date > date(maxdate, '-7 day')
               AND Origin <> 'staking'
           GROUP BY 1
          """
test_Currency_lastTVL = sqldf(q6)

with c2:
    st.markdown(f'''
    ---
    #### Current Composition
    ''')
    plost.donut_chart(
        test_Currency_lastTVL,
        theta='TVL',
        color='Currency'
        )

# Superimpose stacked bargraph with actual TVL
q7 = """ SELECT wk, Currency, TVL - LAG(TVL) OVER (PARTITION BY Currency ORDER BY wk) AS TVL_change
          FROM (
            SELECT date(strftime('%s', Date) - 86400 * ((strftime('%w', Date) + 6) % 7), 'unixepoch') as wk, Currency, AVG("Tokens(USD)") as TVL
              FROM test_all
                WHERE Origin <> 'staking'
              GROUP BY 1,2
            )
          """
test_Currency_wOw = sqldf(q7)
q7p1 = """ SELECT T.*, W.*
             FROM test_Total T LEFT JOIN test_Currency_wOw W ON T.Date = W.wk

"""
test_Currency_wOw_expanded = sqldf(q7p1)
base = alt.Chart(test_Currency_wOw_expanded).encode(
    alt.X('Date:T', axis=alt.Axis(title=None))
)


bar = base.mark_bar().encode(
    alt.Y('sum(TVL_change):Q',axis=alt.Axis(title='TVL Change ($)')),
    color='Currency'
).interactive()
line = base.mark_line().encode(
    alt.Y('TVL:Q',axis=alt.Axis(title='TVL'))
).interactive()
c=alt.layer(bar, line).resolve_scale(
    y='independent'
    )
c3, c4 = st.columns((5,5))
with c3:
    st.markdown(f'''
    #### Week over week change of TVL by currency 
    ''')
    st.altair_chart(c, use_container_width=True)
with c4:
    st.markdown(f'''
    #### %Wise composition of TVL by currency
    ''')
    plost.area_chart(
        test_TVL_currency,
        x='Date',
        y='TVL',
        color='Currency',
        stack='normalize',
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )
    

st.markdown(f'''
---
#### Staking Breakdown by currency
''')

q8 = """ SELECT Date, Currency, sum("Tokens(USD)") as TVL
           FROM test_all
             WHERE Origin = 'staking'
           GROUP BY 1,2
          """
test_Staking_currency = sqldf(q8)
c1, c2 = st.columns((7,3))
with c1:
    st.markdown(f'''
    ---
    #### Summed Staking Composition by Currency
    ''')
    plost.area_chart(
        test_Staking_currency,
        x='Date',
        y='TVL',
        color='Currency',
        stack=True,
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )

q9 = """ SELECT Currency, AVG("Tokens(USD)") as TVL
           FROM test_all, (SELECT max(Date) as maxdate FROM test_origin)
             WHERE Date > date(maxdate, '-7 day')
               AND Origin = 'staking'
           GROUP BY 1
          """
test_Staking_lastTVL = sqldf(q9)

with c2:
    st.markdown(f'''
    ---
    #### Current Staking Composition
    ''')
    plost.donut_chart(
        test_Staking_lastTVL,
        theta='TVL',
        color='Currency'
        )

# Superimpose stacked bargraph with actual TVL
q10 = """ SELECT wk, Currency, TVL - LAG(TVL) OVER (PARTITION BY Currency ORDER BY wk) AS TVL_change
          FROM (
            SELECT date(strftime('%s', Date) - 86400 * ((strftime('%w', Date) + 6) % 7), 'unixepoch') as wk, Currency, AVG("Tokens(USD)") as TVL
              FROM test_all
                WHERE Origin = 'staking'
              GROUP BY 1,2
            )
          """
test_Staking_wOw = sqldf(q10)
q10p1 = """ SELECT T.*, W.*
             FROM test_Total T LEFT JOIN test_Staking_wOw W ON T.Date = W.wk

"""
test_Staking_wOw_expanded = sqldf(q10p1)
base = alt.Chart(test_Staking_wOw_expanded).encode(
    alt.X('Date:T', axis=alt.Axis(title=None))
)


bar = base.mark_bar().encode(
    alt.Y('sum(TVL_change):Q',axis=alt.Axis(title='TVL Change ($)')),
    color='Currency'
).interactive()
line = base.mark_line().encode(
    alt.Y('TVL:Q',axis=alt.Axis(title='TVL'))
).interactive()
c=alt.layer(bar, line).resolve_scale(
    y='independent'
    )
c3, c4 = st.columns((5,5))
with c3:
    st.markdown(f'''
    #### Week over week change of Staking by currency 
    ''')
    st.altair_chart(c, use_container_width=True)
with c4:
    st.markdown(f'''
    #### %Wise composition of Staking by Currency
    ''')
    plost.area_chart(
        test_Staking_currency,
        x='Date',
        y='TVL',
        color='Currency',
        stack='normalize',
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )

q11 = """ SELECT Date, Origin, sum("Tokens(USD)") as TVL
           FROM test_all
             WHERE Origin <> 'staking'
               AND Origin like '%-staking'
           GROUP BY 1,2
          """
test_Staking_chain = sqldf(q11)

c3, c4 = st.columns((5,5))
with c3:
    st.markdown(f'''
    #### Summed Staking Composition by Origin
    ''')
    plost.area_chart(
        test_Staking_chain,
        x='Date',
        y='TVL',
        color='Origin',
        stack=True,
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )
with c4:
    st.markdown(f'''
    #### %Wise composition of Staking by Origin
    ''')
    plost.area_chart(
        test_Staking_chain,
        x='Date',
        y='TVL',
        color='Origin',
        stack='normalize',
        height=400,
        title=test_both['Protocol'][0],
        pan_zoom='both'  # 👈 This is magic!
        )