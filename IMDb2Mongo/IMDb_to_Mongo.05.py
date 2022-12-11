#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import PySimpleGUI as sg
import dns
import copy
import json
import dask.dataframe as dd
import pymongo
import multiprocessing
import warnings
warnings.filterwarnings("ignore")
import imdb
from imdb import IMDb, IMDbError
ia = IMDb()
CPU_COUNT = multiprocessing.cpu_count()


# In[ ]:


def read_table():

    #sg.set_options(auto_size_buttons=True)
    filename = sg.popup_get_file(
        'Dataset to read',
        #no_titlebar=True,
        #grab_anywhere=True,
        file_types=(("CSV Files", "*.csv"),),
        )

    if not filename:
        sg.popup("No filename supplied, exit")
        raise SystemExit("Cancelling: no filename supplied")

    return filename


# In[ ]:


def get_infoset():
    infolist = ia.get_movie_infoset()
    unwanted_infosets = {'main', 'news', 'soundtrack'}
    infolist = [ele for ele in infolist if ele not in unwanted_infosets]
    #define layout
    layout=[[sg.Text('The default infoset is main \n You can add more infosets',size=(30, 2), justification='left')],
        [sg.Listbox(infolist, default_values='', select_mode='extended', key='info', size=(30, 8))],
        [sg.Button('SAVE'), sg.Button('CANCEL')]]
    
    #Define Window
    win =sg.Window('Additional infosets',layout)
    
    #Read  values entered by user
    e,v=win.read()
    strv = ", ".join(v['info'])
    #close first window
    win.close()
    #display string in a popup         
    sg.popup('Chosen infosets:',      
                'main, '+ strv )
    return v['info']


# In[ ]:


def get_database( ):
    # Very basic window.  Return values using auto numbered keys

    layout = [
        [sg.Text('Please enter Database and Collection names')],
        [sg.Text('Connection string', size=(15, 1)), sg.InputText()],
        [sg.Text('Database', size=(15, 1)), sg.InputText()],
        [sg.Text('collection', size=(15, 1)), sg.InputText()],
        [sg.Submit(), sg.Cancel()]
    ]

    window = sg.Window('Database entry window', layout)
    event, values = window.read()
    window.close()
    return values


# In[ ]:


#reads the list of titles from the file and deletes the first two letters of the code
def get_data(filename):
    try:
        titles = pd.read_csv(filename, usecols=[0], names=['_id'])
        titles['_id'] = titles['_id'].str.slice_replace(start=0, stop=2, repl='')
    except:
        sg.popup("The dataset is incorrect, exit")
        raise SystemExit("Cancelling: The dataset is incorrect")
    return titles


# In[ ]:


def identify(DataObj):
    idoc = {}
    tag=''
    if isinstance(DataObj, imdb.Person.Person):
        tag = 'nm'
    elif isinstance(DataObj, imdb.Movie.Movie):
        tag = 'tt'
    elif isinstance(DataObj, imdb.Company.Company):
        tag = 'co'
    else:
        # insert here exception-handling
        pass    
    ID = DataObj.getID()
    idoc['_id'] = tag+str(ID)
    #idoc['id_'] = ID
    return idoc


# In[ ]:


def convert(DataObj):
    document = {}

    classes = (
        imdb.Person.Person,
        imdb.Movie.Movie,
        imdb.Company.Company)

    for key in DataObj.keys():

        if type(DataObj[key]) is dict:
            document[key] = convert(DataObj[key])
            

        elif type(DataObj[key]) is list:
            document.update(identify(DataObj))
            values = DataObj[key]

            if len(values) == 0:
                continue

            sample = values[0]

            if type(sample) in classes:
                val = [x.data for x in values]
                for x in val:
                    n = val.index(x)
                    x.update(identify(values[n]))
                    document[key] = val

            elif len(values) == 1 and type(values[0]) not in classes:
                document[key] = values[0]

            elif len(values) == 1 and type(values[0]) in classes:
                data = values[0].data
                data.update(identify(values[0]))
                document[key] = [data]

            elif type(sample) in (str, bytes):
                document[key] = DataObj[key]

        elif type(DataObj[key]) in classes:
            (DataObj[key]).data.update(identify(DataObj[key]))
            document[key] = convert((DataObj[key]).data)

        else:
            document[key] = DataObj[key]

    return document


# In[ ]:


def append_error_message(error_message):
    """Append error message as a new line at the end of file"""
    # Open the file in append & read mode ('a+')
    with open('errors.txt', "a+") as file_object:
        # Move read cursor to the start of file.
        file_object.seek(0)
        # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0:
            file_object.write("\n")
        # Append text at the end of file
        file_object.write(error_message)


# In[ ]:


#Download the filmography file starting from the title identifier (title) 
#and attributes it (in json format) to the variable movie
def get_main(title, infoset):
    
    try:
        mv = ia.get_movie(title, info = infoset)
#    except (KeyError):
#        new_infoset = copy.copy(infoset)
#        new_infoset.remove('episodes')
#        mv = ia.get_movie(title, info = new_infoset)
    except IMDbError as e:
        append_error_message(str(e))
        movie = None
        return movie
    movie = json.dumps(convert(mv))
    return movie


# In[ ]:


#applies the previous one for each title identifier contained in the dataframe. 
#It works in parallel by taking advantage of the available cores.
def dask_impl(df, infoset):
    from dask.diagnostics import ProgressBar
    ProgressBar().register()
    return dd.from_pandas(df, npartitions=CPU_COUNT).apply(
    lambda row: get_main(
        row._id, infoset),
    axis=1, 
    meta=(int)
  ).compute()


# In[ ]:


#Non-parallel function alternative to the previous one
def apply_impl(df, infoset):
    return df.apply(
        lambda row: get_main(
        row._id, infoset), axis = 1
    )


# In[ ]:


def connect(values, coll=None):

    client = pymongo.MongoClient(values[0])
    db = client[str(values[1])]
    collection = db[str(values[2])]
    return collection


# In[ ]:


def to_mongo(mov, values):
    collection = connect(values)
    pyresponse = json.loads(mov)
    collection.insert_one(pyresponse)


# In[ ]:


def app(df, values):
    try:
        dd.from_pandas(df, npartitions=CPU_COUNT).apply(to_mongo, args=(values,), meta=(int)).compute()
    except:
        sg.popup("Something wrong with the connection, exit")
        raise SystemExit("Cancelling: Something wrong with the connection")


# In[ ]:


def main():
    sg.theme('Material1')      # Add some color to the window

    filename = read_table()
    titles = get_data(filename)
    values = get_database()
    infoset = get_infoset()
    infoset.insert(0, 'main')
    
    layout = [  [sg.Text('Below you can see the download progress:')],
                [sg.Output(size=(60,3))]    ]
    window = sg.Window('Window Title', layout, finalize = True)
    
    df = dask_impl(titles, infoset)
    window.close()
    df.dropna(inplace=True)
    app(df, values)
    sg.popup("Operation completed successfully")


# In[ ]:


main()

