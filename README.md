## deel-martin-de-task

## Pre-requisites
- docker desktop 
- docker compose

## TL;DR
This Application does the following tasks 
- hits apifootball events enpoint and gets matches data 
- writes the data in a postgres database 
- runs transformation scripts to create fact and dimension tables 
- runs queries to answer given business questions

To run the application
1. Create an account at https://apifootball.com/ to get an API key 
2. clone this repo
3. Paste the API key as the value of `API_KEY` environment variable on line 10 in `docker-compose.yml`
4. In your terminal, from the root folder of this repo, run `docker build -t apifootball .` 
5. Once complete, run docker-compose up


## App details 
The app gets matches data from apifootball, stores it in a postgres database, creates facts and dimensions and uses 
them to answer some business questions. For more details on the task, see `de-task.pdf` in the root folder of this 
repo

## Assumptions 
After reading the questions, I made the following assumptions 
- I only considered one league. All the business questions make sense to be done for one league at a time, such as the 
league table, and top scorers. The solution can however work for a multiple table leagues (would have to be redesigned to
accomodate head to head leagues). I chose the English Premier League for this task. 
- To get the entire league data, I got the data from 2022-08-05 to 2023-05-29