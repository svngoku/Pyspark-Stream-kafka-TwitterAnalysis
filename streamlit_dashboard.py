"""
Streamlit Twitter Analysis Dashbaord
"""
import numpy as np
import streamlit as st
import plotly.figure_factory as ff
import pandas as pd
import time 
import plotly.express as px
from wordcloud import WordCloud, STOPWORDS
from pymongo import MongoClient

# Connect to mongo
client = MongoClient(
    "mongodb+srv://twitterbot:SaPDBU5WU3O349mn@cluster0.z5t7h.mongodb.net/?retryWrites=true&w=majority"
)
# Tweets database
twitteranalysis_database = client.twitteranalysis

# Connect to the Twitter Stream collection
twitter_stream = twitteranalysis_database.twitter_stream

topics = ["Harcelements"]

# Streamlit config
st.set_page_config(
    page_title = 'Real-Time Twitter Dashboard Analysis',
    page_icon = '✅',
    layout = 'wide'
)

# Create the tweets_stream dataframe
df = pd.DataFrame({
  'first column': [1, 2, 3, 4],
  'second column': [10, 20, 30, 40]
})

topics_filter = st.selectbox("Select topics", pd.unique(df['tags']))

st.line_chart(df)
