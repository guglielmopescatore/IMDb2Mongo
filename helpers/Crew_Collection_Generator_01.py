#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
import json
import dns


# In[2]:


def aggregation(field):
    #Inserire qui la stringa di connessione a MongoDB con il proprio nome utente e password
    client = pymongo.MongoClient('<Atlas connection string>')
    #Indicare qui il nome del database e della collection generati da IMDb2Mongo
    result = client['<Database>']['<Collection>'].aggregate([
    {
        '$match': {
            f'{field}': {
                '$exists': True, 
                '$ne': []
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            f'{field}': 1, 
            'year': 1
        }
    }, {
        '$unwind': {
            'path': f'${field}'
        }
    }, {
        '$addFields':{
        f"{field}.code": f'${field}._id',
        f"{field}.role": f'{field}',
        f"{field}.year": '$year'
    }
    }, {
        '$replaceRoot': {
            'newRoot': f'${field}'
        }
    },{
        '$project': {
            '_id': 0
        }
    },{
        '$merge': {
# Inserire qui il nome della Crew Collection che verrà salvata su MongoDB
            'into':'<Crew Collection Name>'
        }
    }
]) 


# In[3]:


def main():
    # I fields vanno controllati e eventualmente aggiunti i nuovi
    fields = ['art department', 'art direction', 'assistant director', 'camera and electrical department', 'cast', 'casting department', 'casting director', 'cinematographer', 'composer', 'costume department', 'costume designer', 'creator', 'director', 'editor', 'editorial department', 'location management', 'make up', 'miscellaneous crew', 'music department', 'producer', 'production design', 'production manager', 'script department', 'set decoration', 'sound crew', 'special effects', 'stunt performer', 'visual effects', 'writer']
    for field in fields:
        aggregation(field)


# In[4]:


main()

