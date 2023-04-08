"""
Author: Vikrant Palle
"""

import psycopg2
import psycopg2.extensions
from datetime import datetime,timedelta,time
from dateutil import tz
from flask import Flask,request,send_file
from flask_cors import CORS,cross_origin
import bisect
import uuid
import threading
import os.path
import csv

api = Flask(__name__)
cors = CORS(api)
api.config['CORS_HEADERS'] = 'Content-Type'

CSV_OUTPUT_DIR = 'C:/Users/vikra/loop-store monitoring/static/'

try:
    db = psycopg2.connect("postgres://postgres:postgres@localhost:5432/loop")
except psycopg2.OperationalError as err:
    raise ConnectionError(err) from None

reports_lock = threading.Lock()
reports = dict()    


def get_business_hours(cur: psycopg2.extensions.cursor, store_id: str) -> list[tuple]:
    """retrieves the business hours of the store

    Args:
        cur (cursor): cursor object of database
        store_id (str): id of the store

    Returns:
        list[tuple]: list of all business hours in a week in the format (day(int),start_time_local(datetime),end_time_local(datetime)) where day = 0-6(0 for Monday)
    """
    cur.execute("select day,start_time_local,end_time_local from businesshours where store_id = %s;",(store_id,))
    businessHours  = cur.fetchall()
    
    # if business hours for the store is missing assume it stays open 24*7 
    if businessHours == None:
        for idx in range(7):
            businessHours.append((idx,time(0,0),time(23,59)))             
    
    return businessHours  


def get_timezone(cur: psycopg2.extensions.cursor,store_id: str) -> str:
    """retrieves timezone of a store

    Args:
        cur (cursor): cursor object of database
        store_id (str): id of the store

    Returns:
        tzfile: timezone of store
    """
    cur.execute("select store_id,timezone_str from storetimezone where store_id = %s;", (store_id,))
    storetz = cur.fetchone()
    
    # if timezone data is invalid or missing assume timezone to be America/Chicago
    if storetz == None:
        return tz.gettz("America/Chicago")
    else:
        return tz.gettz(storetz[1]) if tz.gettz(storetz[1])!=None else tz.gettz("America/Chicago")
        
def get_polling_data(cur: psycopg2.extensions.cursor,store_id: str, start: datetime, end: datetime) -> list[tuple]:
    """retrieves all polls of a store in a time range 

    Args:
        cur (cursor): cursor object of database
        store_id (str): id of the store
        start (datetime): all polls should have a timestamp >= start
        end (datetime): all polls should have a timestamp < end

    Returns:
        list[tuple]: list of polls in the fomat (status(str), timestamp_utc(datetime))
    """ 
    cur.execute("select status,timestamp_utc from storestatus where store_id = %s and timestamp_utc >= %s and timestamp_utc < %s order by timestamp_utc asc", (store_id,start,end,))    
    data = cur.fetchall()
     
    return data if data != None else []    

def transform_poll_data(pollData, start, end, to_tz):
    """retrieves all poll data between the timerange (start,end) and converts the utc timestamp to local zone timestamp 

    Args:
        pollData (list): list of poll data sorted by timestamp
        start (datetime): start of the timerange
        end (datetime): end of the timerange
        to_tz (tzfile): local zone of store

    Returns:
        list: sorted poll data in the timerange (start,end) converted to local time
    """
    # initially the store is assume to be inactive until next poll
    startPoll = ('inactive',start)     
    endPoll = ('inactive',end) 
    tPollData = pollData[bisect.bisect_left(pollData,start,key=lambda x: x[1]):]
    tPollData.insert(0,startPoll)
    tPollData.append(endPoll)
    return list(map(lambda x: (x[0],x[1].replace(tzinfo = tz.gettz('UTC')).astimezone(to_tz)),tPollData))
    
def transform(x,start: datetime,local):
    """converts the businessHours time object to datetime by attaching a date relative to the start datetime 

    Args:
        x (tuple): a businessHour segment in the format (day,start_time,end_time)
        start (datetime): the start time of the week
        local (tzfile): local time zone of the store

    Returns:
        tuple: businessHour in the format (start_date_time,end_date_time) in local time zone
    """
    start = start.replace(tzinfo=tz.gettz('UTC')).astimezone(local)
    if (x[0],x[1])<(start.weekday(),start.time()):
        return (datetime.combine((start + timedelta(days=7-start.weekday()+x[0])).date(),x[1]).replace(tzinfo=local),datetime.combine((start + timedelta(days=7-start.weekday()+x[0])).date(),x[2]).replace(tzinfo=local))
    else:
        return (datetime.combine((start + timedelta(days=x[0] - start.weekday())).date(),x[1]).replace(tzinfo=local),datetime.combine((start + timedelta(days=x[0] - start.weekday())).date(),x[2]).replace(tzinfo=local))
        
def calculateOverlap(polls,businessHours):
    """calculates overlap between business hours and uptime
    
    The poll data is divided into segments and the assumptions that the 
    state of the store persists between two polls has been made. Eg: 
    if there are two polls ('active',t1) and ('inactive',t2) and t2>t1 
    then for all t1 <= t < t2 the status is 'active'. The poll data has
    been converted from utc to local time. 
    
    The business hours have been transformed from time to datetime,i.e,
    they have dates of the week attached to them which allows comparison 
    between polls and business hours. 
    
    Sliding window algorithm has been used to calculate the overlap. 
    Initially the window is polls[l] and polls[l+1] where l=0. The 
    business hours are iterated through and the overlap between 
    business hours and the window is calculated and based on the 
    status of polls[l] added to either active or inactive time.
    When the start time of business hours > polls[l+1],i.e, then 
    all business hours after this point will not overlap with current 
    window. Hence we move to next segment (l=l+1) and continue
    iterating through business hours until it is exhausted.
    Time complexity: O(len(businessHours))
    Linear time complexity
    
    Note: For this algorithm to work it is important that no 
    overlap between business hours exist.

    Args:
        polls (list[(str,datetime)]): list of poll data in the format (status,timestamp) sorted by timestamp
        businessHours (list[(datetime,datetime)]): list of business hours in the format (start_time,end_time) sorted by start_time

    Returns:
        (float,float): uptime and downtime respectively in minutes
    """
    na = len(polls)
    nb = len(businessHours)
    act,inact,l,ind = 0,0,0,0
    while(l<na-1):
        status = polls[l][0]
        while ind < nb and businessHours[ind][0] < polls[l+1][1]:
            if businessHours[ind][1] < polls[l][1]: 
                ind+=1
                continue
            if status == 'active':
                act += (min(businessHours[ind][1],polls[l+1][1]) - max(businessHours[ind][0],polls[l][1])).total_seconds()                    
            else:
                inact += (min(businessHours[ind][1],polls[l+1][1]) - max(businessHours[ind][0],polls[l][1])).total_seconds()
            ind += 1
        if (ind): ind -= 1
        l+=1  
    # convert seconds to minutes          
    return (act / 60,inact / 60) 

def calculateStoreActivity(cursor,store_id):
    """calculates uptime and downtime for last_week,last_day and last_hour

    Args:
        cursor (cursor): cursor object of database
        store_id (str): id of the store

    Returns:
        list: contains store_id,uptime_last_hour(in minutes),uptime_last_day(in hours),uptime_last_week(in hours),downtime_last_hour(in minutes),downtime_last_day(in hours),downtime_last_week(in hours) respectively
    """
    business_hours = get_business_hours(cursor, store_id)
    localZone = get_timezone(cursor, store_id)
    endTime = datetime.utcnow()
    poll_data = get_polling_data(cursor,store_id,endTime - timedelta(weeks=1),endTime)
    
    # past week,day,hour uptime and downtime
    past_week = calculateOverlap(transform_poll_data(poll_data,endTime - timedelta(weeks=1),endTime,localZone),sorted(list(map(lambda x: transform(x,endTime - timedelta(weeks=1),localZone),business_hours))))
    past_day = calculateOverlap(transform_poll_data(poll_data,endTime - timedelta(days=1),endTime,localZone),sorted(list(map(lambda x: transform(x,endTime - timedelta(days=1),localZone),business_hours))))
    past_hour = calculateOverlap(transform_poll_data(poll_data,endTime - timedelta(hours=1),endTime,localZone),sorted(list(map(lambda x: transform(x,endTime - timedelta(hours=1),localZone),business_hours))))
    
    return [store_id,past_hour[0],past_day[0]/60,past_week[0]/60,past_hour[1],past_day[1]/60,past_week[1]/60]
    
def generate_report(report_id):
    """generates report and creates a new csv file with filename=report_id.csv with all required data

    Args:
        report_id (str): unique id to identify the report
    """
    cur = db.cursor()
    cur.execute("select store_id from storetimezone limit 10;")
    store_ids = cur.fetchall()
    with open(os.path.join(CSV_OUTPUT_DIR,f"{report_id}.csv"),'w',encoding='UTF-8',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_day(in hours)', 'uptime_last_week(in hours)', 'downtime_last_hour(in minutes)', 'downtime_last_day(in hours)', 'downtime_last_week(in hours)'])
        for store_id in store_ids:
            writer.writerow(calculateStoreActivity(cur,store_id[0]))
    cur.close()        
    with reports_lock:    
        reports[report_id] = True  

@api.route("/trigger_report")
@cross_origin()
def trigger_report():
    report_id = uuid.uuid4().hex
    with reports_lock:
        reports[report_id] = False
    thr = threading.Thread(target=generate_report,args=(report_id,))
    thr.start()
    return report_id   

@api.route("/get_report")
@cross_origin()
def get_report():
     id = request.args["id"]
     with reports_lock:
        if id not in reports: return "invalid id" 
        if reports[id]:
            return send_file(os.path.join(CSV_OUTPUT_DIR,f"{id}.csv"))
        else:
            return "running"
       

if __name__ == "__main__":
    api.run()