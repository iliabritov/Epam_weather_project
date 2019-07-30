To start working with the application run (no Docker):
  1. create database in postgresql using commands from file db_creation.sql
  2. set your user and password in file 'app/db_config.json'  
  3. run storage.py
  4. run app.py
  5. go to http://127.0.0.1:5000/


The database contains data from 01.01.2010 to 15.01.2019
Data sources: 
	- www.rp5.ru
	- www.worldweatheronline.com

Date format: dd.mm.yyyy
Example: 23.03.2013

List of avaible cities:
- Samara 
- Moscow
- Kazan
- Saint Petersburg
- Vologda
- Kirov
- Perm
- Omsk
- Irkutsk
- Krasnoyarsk
- London
- Nur-Sultan
- Minsk
- Warsaw
- Berlin
- Canberra
