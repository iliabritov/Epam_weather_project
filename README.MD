### What is it?

###### Description
This is a simple educational project (from Epam training center) on python3 that allows you to collect weather data and get weather statistics for a selected city.

- **Input data:** start data, finish data, city
- **Output data:**
  - min, max, average temp
  - precipitation info (most frequent, procent of days)
  - wind speed and direction

###### Used tools and technologies
- Python3
- Flask
- PostgreSQL
- Docker

### How to run
###### Docker
  1. Download project using
  > git clone https://github.com/iliabritov/Epam_weather_project
  2. Go to clone folder
  > cd Epam_weather_project
  2. Be sure that you have Docker :) (download and install from [here](https://www.docker.com/get-started))
  3. Run project
  > docker-compose up -d
  4. go to http://localhost:5000/ (you need to wait - the data is loaded on average 1.5 - 2 minutes)

### The database contains data from **01.01.2010** to **15.01.2019**

### Data sources: 
- www.rp5.ru
- www.worldweatheronline.com


###### Date format:
  dd.mm.yyyy
###### Example:
  23.03.2013

### List of avaible cities:
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
