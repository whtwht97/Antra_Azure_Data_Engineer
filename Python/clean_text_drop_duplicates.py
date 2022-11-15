#!/usr/bin/env python
# coding: utf-8

# In[31]:


import numpy as np
import pandas as pd


# In[32]:


p1 = pd.read_csv("people_1.txt",delimiter = "\t")
p1


# In[33]:


def clean(df):
    df = df.apply(lambda x : x.str.strip())
    df[['FirstName','LastName']] = df[['FirstName','LastName']].apply(lambda x : x.str.capitalize())
    df["Phone"]=df["Phone"].apply(lambda x: x.replace("-",""))
    df['Address'] = df["Address"].apply(lambda x: x.replace('No.','').replace('#',''))
    
    df = df.drop_duplicates()
    return df 


# In[34]:


cleaned_p1 = clean(p1)
cleaned_p1


# In[35]:


p2 = pd.read_csv("people_2.txt",delimiter = "\t")
p2


# In[36]:


clean_p2 = clean(p2)
clean_p2

