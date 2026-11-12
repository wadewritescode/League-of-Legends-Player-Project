import os
from dotenv import load_dotenv
import requests 
import sqlalchemy
import pandas as pd
import polars
import numpy as np
import urllib.parse
import json
import datetime as dt
import logging
import click 


logger = logging.Logger(__name__)
logger.info('testing logger')
load_dotenv()
headers = {"X-Riot-Token": os.environ['API_KEY']}



@click.command()
@click.option('--gamename', help='The name of the person in game')
@click.option('--tagline', help='The characters after the hash in a username')
@click.option('--gamecount', help='how many games do you want')
@click.option('--serverlocation', help='What server does the player play on?')
def process_api_data(gamename, tagline, gamecount, serverlocation) :
    
    def get_user_details (gamename, tagline):
        """RETURNS THE RIOT SPECIFIC USER ID FOR THE ACCOUNT PASSED IN"""
        print(f'Getting User Data for {gamename}')
        inputs = {'gameName' : gamename, 'tagLine': tagline}
        riot_url = 'https://americas.api.riotgames.com'
        user_endpoint_url =f'/riot/account/v1/accounts/by-riot-id/{inputs['gameName']}/{inputs['tagLine']}'
        return requests.get(riot_url + user_endpoint_url, headers=headers).json()['puuid']

    def get_match_data(user_puuid, gamecount, serverlocation):
        """RETURNS THE DETAILS FOR THE LAST N AMOUNT OF GAMES FOR THE SPECIFIC USER ID"""
        print(f'Getting Match Data for {gamename}')
        new_riot_url = f'https://{serverlocation}.api.riotgames.com'
        match_id_endpoint_url = f'/lol/match/v5/matches/by-puuid/{user_puuid}/ids'
        payload = {'count': gamecount}
        result = requests.get(new_riot_url + match_id_endpoint_url, headers=headers, params=payload)
        if result.status_code !=200:
            return print(f'Failed. Error Code {result.status_code}')
        else: 
            result = result.json()
        return [requests.get(new_riot_url + f'/lol/match/v5/matches/{matchId}', headers=headers).json() for matchId in result]
        
    def collate_match_data(gameList) :
        print(f'Collating Match Data for {gamename}')
        listy = []
        stat_columns = ['puuid','win', 'championName', 'positionAssignedByMatchmaking', 'totalDamageDealt', 'totalMinionsKilled', 'goldEarned', 'kills', 'deaths', 'assists', 'doubleKills', 'tripleKills', 'quadraKills', 'pentaKills', 'wardsPlaced', 'visionWardsBoughtInGame', 'wardsKilled','basicPings', 'firstBloodKill', 'totalTimeCCDealt', 'longestTimeSpentLiving','timePlayed']
        for game in gameList:
            diccy = {}
            for n in range(0, len(game['info']['participants'])):
                d = {}
                d['matchId'] = game['metadata']['matchId']
                d['datapulldate'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for col in stat_columns:
                    d[f"{col}"] = game['info']['participants'][n][col]
                
                diccy[game['info']['participants'][n]['riotIdGameName']] = d
            
            listy.append(pd.DataFrame.from_dict(diccy, orient='index').reset_index().rename(columns={'index':'riotIdGameName'}))
        
        df = pd.concat(listy, ignore_index=True)
        print('writing match data')
        df.to_csv(f'data\gameList_Export_{dt.datetime.now().strftime('%Y%m%d_%H_%M')}.csv',mode='w' ) 
            
            
    user = get_user_details(gamename, tagline)
    games = get_match_data(user, gamecount, serverlocation)
    collate_match_data(games)
    print('All done')       

if __name__ == "__main__":
    process_api_data()