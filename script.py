from moodle import Moodle
from moodle.core.user import criteria
import pandas as pd
# from google.colab import auth

import gspread
from gspread_pandas import Spread, Client, conf
# from oauth2client.client import GoogleCredentials
import datetime
import requests

import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import markdown
from time import sleep
import os
import pytz
from dotenv import load_dotenv

load_dotenv()

CWD = os.getenv("CWD")
MOODLE_TOKEN = os.getenv("MOODLE_TOKEN")
EXPORT_SHEET_TOKEN = os.getenv("EXPORT_SHEET_TOKEN")
GOOGLE_AUTH_FILENAME = os.getenv("GOOGLE_AUTH_FILENAME")
GOOGLE_CHAT_WEBHOOK = os.getenv("GOOGLE_CHAT_WEBHOOK")

url = 'https://egitim.stgm.org.tr/webservice/rest/server.php'
moodle = Moodle(url, MOODLE_TOKEN)
dict_site_info = moodle('core_webservice_get_site_info')
site_info = moodle.core.webservice.get_site_info()  # return typed site_info

gc = gspread.service_account(CWD + GOOGLE_AUTH_FILENAME)
workbook = Spread(EXPORT_SHEET_TOKEN, creds=gc.auth)

def get_date_from_timestamp(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

enrolled_user_fields = [{'name': 'userfields', 'value': 'email'},
                        {'name': 'userfields', 'value': 'username'},
                        {'name': 'userfields', 'value': 'customfields'},
                        {'name': 'userfields', 'value': 'firstname'},
                        {'name': 'userfields', 'value': 'lastname'},
                        {'name': 'userfields', 'value': 'enrolled_courses'},
                        {'name': 'userfields', 'value': 'firstaccess'},
                        {'name': 'userfields', 'value': 'lastaccess'},
                        {'name': 'userfields', 'value': 'lastcourseaccess'},
                        {'name': 'userfields', 'value': 'roles'},
                        
                        ]

# ALL USERS

def get_all_users():
    all_users = moodle.post("core_user_get_users", criteria=[{"key": "email", "value": "%%"}])

    custom_fields = [{"customfields": all_users["users"][x]["customfields"],
                    "id": all_users["users"][x]["id"],
                    "auth": all_users["users"][x]["auth"],
                    "email": all_users["users"][x]["email"],
                    "username": all_users["users"][x]["username"],
                    "firstaccess": all_users["users"][x]["firstaccess"],
                    "lastaccess": all_users["users"][x]["lastaccess"],
                    "fullname": all_users["users"][x]["fullname"]} for x in range(0, len(all_users["users"]))]
    custom_fields_list = []
    for record in custom_fields:
        custom_fields_dict = {
            "id": record["id"],
            "username": record["username"],
            "fullname": record["fullname"],
            "email": record["email"],
            "auth": record["auth"],
            "firstaccess": get_date_from_timestamp(record["firstaccess"]),
            "lastaccess": get_date_from_timestamp(record["lastaccess"])
        }
        for field in record["customfields"]:
            custom_fields_dict[field["shortname"]] = field["value"]
        # print(custom_fields_dict)
        custom_fields_list.append(custom_fields_dict)

    custom_fields_pd = pd.DataFrame(custom_fields_list)
    workbook.df_to_sheet(custom_fields_pd, index=False, sheet="All Users", start="A2", replace=True)
    # all_users_sheet.update_cells([custom_fields_pd.columns.values.tolist()] + custom_fields_pd.values.tolist())
    return custom_fields_pd

def get_user_completion(courseid: int, userid: int, activities_dict: dict):
    user_course_completion = moodle.post("core_completion_get_activities_completion_status", courseid=courseid, userid=userid)

    completions = {}
    for record in user_course_completion["statuses"]:
        id = record["cmid"]
        state = record["state"]
        name = activities_dict[id]
        completions[id] = {"name": name, "state": state}
    return completions

def process_enrolled_users_of_course(courseid: int, sheet_name: str):
    enrolled_users = moodle.post("core_enrol_get_enrolled_users", courseid=courseid)
    len(enrolled_users)
    enrolled_users_pd = pd.DataFrame(enrolled_users)
    enrolled_users_pd.drop(columns=["customfields", "department", "description", "descriptionformat", "country", "profileimageurlsmall", "profileimageurl", "groups", "enrolledcourses"],
                        inplace=True)
    # enrolled_users_pd = pd.merge(enrolled_users_pd, custom_fields_pd[["id", "email", "organization"]], how="left", left_on="id", right_on="id")
    enrolled_users_pd["roles"] = enrolled_users_pd["roles"].apply(lambda x: x[0]["shortname"])
    enrolled_users_pd["firstaccess"] = enrolled_users_pd["firstaccess"].apply(get_date_from_timestamp)
    enrolled_users_pd["lastaccess"] = enrolled_users_pd["lastaccess"].apply(get_date_from_timestamp)
    enrolled_users_pd["lastcourseaccess"] = enrolled_users_pd["lastcourseaccess"].apply(get_date_from_timestamp)

    activity_ids = moodle.post("core_completion_get_activities_completion_status", courseid=courseid, userid=enrolled_users_pd["id"][0])
    activity_ids = [activity["cmid"] for activity in activity_ids["statuses"]]
    activity_names = [moodle.post("core_course_get_course_module", cmid=id)["cm"]["name"] for id in activity_ids]
    activities_dict = dict(zip(activity_ids, activity_names))
    print(activities_dict)

    for activity_id in activities_dict.keys():
        enrolled_users_pd[activities_dict[activity_id]] = None

    for index, user in enrolled_users_pd.iterrows():
        completions = get_user_completion(courseid=courseid, userid=user["id"], activities_dict=activities_dict)
        for activity_id in activities_dict.keys():
            activity_name = activities_dict[activity_id]
            completion = completions[activity_id]["state"]
            print(index, user["fullname"], completion)
            enrolled_users_pd.loc[index, activity_name] = completion
            # enrolled_users_pd[activity_name][user[1]["id"]] = completions[activity_id]["state"]

    workbook.df_to_sheet(enrolled_users_pd, index=False, sheet=sheet_name, start="A2", replace=True)
    return enrolled_users_pd

def mark_completed_time():
    completed_time = datetime.datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d %H:%M:%S")
    gc.open_by_key(EXPORT_SHEET_TOKEN).worksheet("META").update("B1", completed_time)
    return completed_time

def send_message_to_google_chat(message):
    message = requests.post(
        url=GOOGLE_CHAT_WEBHOOK,
        # headers=webhook_headers,
        data=json.dumps({"text": message})
    )
    return message

def get_total_users(custom_fields_pd):
    return custom_fields_pd.shape[0]

def get_total_completed_users(enrolled_users_pd):
    return enrolled_users_pd["Katılım Belgesi"].sum()

def prepare_message(completed_time: str, custom_fields_pd: pd.DataFrame, enrolled_users_pd_32, enrolled_users_pd_33, enrolled_users_pd_34):
    message_text = f"""
        Moodle kullanıcı bilgileri güncellendi ({completed_time}).
        Toplam kullanıcı: {get_total_users(custom_fields_pd)}

        Herkese Lazım Dersler
        Kayıtlı kişi: {get_total_users(enrolled_users_pd_32)}
        Tamamlayan: {get_total_completed_users(enrolled_users_pd_32)}

        Herkes Plan Sever
        Kayıtlı kişi: {get_total_users(enrolled_users_pd_33)}
        Tamamlayan: {get_total_completed_users(enrolled_users_pd_33)}

        Herkes Dijital Sever
        Kayıtlı kişi: {get_total_users(enrolled_users_pd_34)}
        Tamamlayan: {get_total_completed_users(enrolled_users_pd_34)}
    """
    return message_text

if __name__ == "__main__":
    custom_fields_pd = get_all_users()
    print("Exported all users")
    enrolled_users_pd_32 = process_enrolled_users_of_course(32, "Herkese Lazım Dersler")
    print("Exported Herkese Lazım Dersler")
    enrolled_users_pd_33 = process_enrolled_users_of_course(33, "Herkes Plan Sever")
    print("Exported Herkes Plan Sever")
    enrolled_users_pd_34 = process_enrolled_users_of_course(34, "Herkes Dijital Sever")
    print("Exported Herkes Dijital Sever")

