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
def get_match_ids (gamename, tagline,gamecount, serverlocation):
    """RETURNS THE RIOT SPECIFIC USER ID FOR THE ACCOUNT PASSED IN"""
    
    def get_user_id(gamename, tagline):
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
        print(user_puuid)
        payload = {'count': gamecount}
        result = requests.get(new_riot_url + match_id_endpoint_url, headers=headers, params=payload)
        if result.status_code !=200:
            return print(f'Failed. Error Code {result.status_code}')
        else: 
            result = result.json()
    
        return [requests.get(new_riot_url + f'/lol/match/v5/matches/{matchId}/timeline', headers=headers).json() for matchId in result]
    
    with open('hoya_punani.json' , 'w') as e:
        json.dump(get_match_data(get_user_id(gamename, tagline), gamecount, serverlocation), e, indent=10)


if __name__ == "__main__":
    get_match_ids()
