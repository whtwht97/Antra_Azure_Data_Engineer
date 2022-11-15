#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os 
import pandas as pd
import json


# In[3]:


movie = pd.read_json('movie.json')
movie


# In[ ]:


# split to 8 files 

with open('movie.json', 'r', encoding='utf-8') as file:
    js= json.load(file)

chunks = 8
movies = js['movie']
movies_per_chunk = len(movies)// chunks

for current_chunk in range(chunks):
    with open('movie_' + str(current_chunk) + '.json', 'w') as outfile:
        to_write = {
            'movie': movies[current_chunk * movies_per_chunk:(current_chunk + 1) * movies_per_chunk]
        }
        json.dump(to_write, outfile, indent = 4)

