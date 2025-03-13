
import base64
import json
import multiprocessing
import os
import shutil
from typing import Optional
from constants import *
from decouple import config
import requests as rq
import pandas as pd
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# from minioservice import MinioService


def get_logger():
    """Get the logger."""
    try:
        # Configuration du logger
        logger = logging.getLogger(APP_NAME)
        logger.setLevel(logging.DEBUG)

        # Formatter pour le log
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        if not os.path.exists(LOG_DIRECTORY):
            os.makedirs(LOG_DIRECTORY)

        # Création d'un gestionnaire de fichiers rotatif basé sur la date
        log_filename = datetime.now().strftime("./logs/%Y-%m-%d.log")
        file_handler = TimedRotatingFileHandler(
            log_filename, when="midnight", interval=1, backupCount=3)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        # Ajout du gestionnaire de fichiers au logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger
    except Exception as e:
        print("Error while getting logger: ", e)
        return None


def load_json(file_path: str):
    """Load a JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        return None


def save_json(data: dict, file_path: str):
    """Save a JSON file."""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        return False


def get_env_var(var_name: str):
    """Get an environment variable, or return a default value."""
    return config(var_name)


def get_env_vars():
    """Get all environment variables."""
    evars = {key: get_env_var(key) for key in KEYS}
    evars['logger'] = get_logger()
    return evars


def convert_iso_to_yyyy_mm_dd(iso_date):
    """
    Converts an ISO 8601 date string (with "Z") to YYYY-MM-DD format.

    :param iso_date: Date string in ISO 8601 format (e.g., "2025-02-24T00:00:00.000Z")
    :return: Date in YYYY-MM-DD format
    """
    # Remove the "Z" and convert to a datetime object
    date_obj = datetime.strptime(
        iso_date.replace("Z", ""), "%Y-%m-%dT%H:%M:%S.%f")

    # Format the date to YYYY-MM-DD
    return date_obj.strftime("%Y-%m-%d")


def convert_iso_to_yyyy_mm_dd_hh_mm_ss(iso_date):
    """
    Converts an ISO 8601 date string (with "Z") to YYYY-MM-DD HH:MM:SS format.

    :param iso_date: Date string in ISO 8601 format (e.g., "2025-02-24T00:00:00.000Z")
    :return: Date in YYYY-MM-DD HH:MM:SS format
    """
    # Remove the "Z" and convert to a datetime object
    date_obj = datetime.strptime(
        iso_date.replace("Z", ""), "%Y-%m-%dT%H:%M:%S.%f")

    # Format the date to YYYY-MM-DD HH:MM:SS
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")


def make_get_request(base_url: str, endpoint: str, headers: dict):
    """Make a GET request."""
    try:
        urlcomplete = base_url + endpoint
        res = rq.request("GET", urlcomplete, headers=headers)
        if res.status_code == 200:
            return res
    except Exception as e:
        return None


def make_post_request(base_url: str, endpoint: str, headers: dict, payload: dict):
    """Make a POST request."""
    try:
        urlcomplete = base_url + endpoint
        res = rq.request("POST", urlcomplete, headers=headers, data=payload)
        if res.status_code == 201:
            return res
    except Exception as e:
        return None


def make_patch_request(base_url: str, endpoint: str, headers: dict, payload: dict):
    """Make a PATCH request."""
    try:
        urlcomplete = base_url + endpoint
        res = rq.request("PATCH", urlcomplete, headers=headers, data=payload)
        print(res)
        if res.status_code == 200:
            return res
    except Exception as e:
        return None


def generate_activity_report(
    minioservice,
    report_data: list,
    output_file: str = 'rapport_activites.xlsx'
):
    """
    Génère un fichier Excel de rapport de l'activité.

    :param report_data: Données de l'activité
    :param output_file: Nom du fichier de sortie
    :return: Nom du fichier de sortie
    """
    # Extraire les champs du premier élément de report_data
    fields = report_data[0].get("fields", [])

    # Initialiser les colonnes
    columns = []
    # Extraire les labels et les types des champs
    columns = [
        f.get("label")
        for f in fields
        if f.get("label")
    ]  # Ignorer les labels nuls

    print("Colonnes extraites :", columns)

    # Ajouter 'HORODATEUR' au début de la liste des colonnes
    columns = ['HORODATEUR'] + columns
    print("Colonnes finales :", columns)

    datas = []
    for rd in report_data:
        d = rd.get("fields")
        td = {
            item["label"]: ",".join([
                minioservice.get_file_url(v)
                for v in str(item["value"]).split(",")
            ])
            if item["type"] in MINIO_TYPES else item["value"]
            for item in d
        }
        # td = {
        #     item["label"]: item["value"]
        #     for item in d
        # }
        td["HORODATEUR"] = convert_iso_to_yyyy_mm_dd_hh_mm_ss(
            str(rd.get("createdAt"))
        )
        datas.append(td)
    df = pd.DataFrame(datas, columns=columns)
    output_filepath = f'./temp/{output_file}'
    df.to_excel(
        output_filepath,
        index=False
    )

    print(f"Rapport généré : {output_file}")
    return output_filepath


def core_login(env_vars: dict):
    """Login to CORE."""
    logger = env_vars.get('logger')
    base_error = "Error while logging in to CORE: "
    error = ""
    try:
        base_url = env_vars.get('CORE_URL') + \
            env_vars.get('ROUTE_OF_CORE_FOR_AUTH')

        res = make_post_request(
            base_url=base_url,
            endpoint="/signin",
            headers={
                "Content-Type": "application/json"
            },
            payload=json.dumps({
                "email": env_vars.get('SYS_USERNAME'),
                "password": env_vars.get('SYS_PASSWORD')
            })
        )
        if res:
            return {
                "success": True,
                "data": res.json().get("data")
            }
        else:
            return {
                "success": False,
                "error": res.json().get("message")
            }
    except Exception as e:
        error = str(e)
        logger.error(
            LOG_CONST.format(
                base_error,
                error
            )
        )


def core_list_report_not_delivered(env_vars: dict, jwt_token: str):
    """List reports not delivered."""
    logger = env_vars.get('logger')
    base_error = "Error while listing reports not delivered: "
    error = ""
    try:
        base_url = env_vars.get("CORE_URL") + \
            env_vars.get('ROUTE_OF_CORE_FOR_REPORT')

        res = make_get_request(
            base_url=base_url,
            endpoint="/not-delivered",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {jwt_token}"
            }
        )
        if res:
            return {
                "success": True,
                "data": res.json().get("data")
            }
        else:
            return {
                "success": False,
                "error": res.json().get("message")
            }
    except Exception as e:
        error = str(e)
        logger.error(
            LOG_CONST.format(
                base_error,
                error
            )
        )


def core_get_activity_data(env_vars: dict, jwt_token: str, report: dict):
    """Get activity data."""
    logger = env_vars.get('logger')
    base_error = "Error while getting activity data: "
    error = ""
    try:
        base_url = env_vars.get('CORE_URL') + \
            env_vars.get('ROUTE_OF_CORE_FOR_STORE')

        res = make_post_request(
            base_url=base_url,
            endpoint="/for-runner",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {jwt_token}"
            },
            payload=json.dumps({
                "activityId": report.get('activityId'),
                "startDate": convert_iso_to_yyyy_mm_dd(report.get('startDate')),
                "endDate": convert_iso_to_yyyy_mm_dd(report.get('endDate'))
            })
        )
        if res:
            return {
                "success": True,
                "data": res.json().get("data")
            }
        else:
            return {
                "success": False,
                "error": res.json().get("message")
            }
    except Exception as e:
        error = str(e)
        logger.error(
            LOG_CONST.format(
                base_error,
                error
            )
        )


def core_deliver_report(env_vars: dict, jwt_token: str, report: dict):
    """Deliver a report."""
    logger = env_vars.get('logger')
    base_error = "Error while delivering report: "
    error = ""
    try:
        base_url = env_vars.get('CORE_URL') + \
            env_vars.get('ROUTE_OF_CORE_FOR_REPORT')

        res = make_patch_request(
            base_url=base_url,
            endpoint=f"/{report.get('id')}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {jwt_token}"
            },
            payload=json.dumps({
                "filename": report.get('filename'),
                "filelink": report.get('filelink'),
            })
        )
        if res:
            return {
                "success": True,
                "data": res.json().get("data")
            }
        else:
            return {
                "success": False,
                "error": res.json().get("message")
            }
    except Exception as e:
        error = str(e)
        logger.error(
            LOG_CONST.format(
                base_error,
                error
            )
        )
