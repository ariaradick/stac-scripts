import sys,os
import json
import requests

def search_catalog(**kwargs):
    for key,val in kwargs.items():
        print(key,val)