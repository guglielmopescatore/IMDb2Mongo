# IMDb2Mongo
An application that allows creating a Collection in a MongoDB database with the filmography data found in IMDb from a list of titles
## Features
* The list of titles is provided in csv format (IMDb id only)
* Filmographic data are downloaded using the Cinemagoer library https://github.com/cinemagoer/cinemagoer
* The data are transferred to a MongoDB collection. It is necessary to provide the MongoDB connection string, the Database name, and the Collection name
* Any errors in downloading data are recorded in the Errors file
* There is an executable GUI version (PySimpleGUI/PyInstaller) available for Windows
