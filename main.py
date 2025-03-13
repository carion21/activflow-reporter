from time import sleep, time
from joblib import Parallel, delayed
from minioservice import MinioService

from constants import *
from functions import *
from utilities import *

env_vars = get_env_vars()
logger = env_vars.get("logger")

# print('env_vars:', env_vars)

sys_waitime = 30

while True:

    r_core_login = core_login(env_vars)

    jwt_token = None
    if r_core_login.get("success"):
        jwt_token = r_core_login.get("data").get("jwt")

    reports = []
    r_core_reports = core_list_report_not_delivered(env_vars, jwt_token)
    if r_core_reports.get("success"):
        reports = r_core_reports.get("data")

        if len(reports) == 0:
            print("Waiting for next iteration in", sys_waitime, "seconds...")
            sleep(sys_waitime)

            continue

        for report in reports:
            print(report)
            treat_report(env_vars, jwt_token, report)

        # use joblib to parallelize the treatment of reports
        # Parallel(n_jobs=N_PROCESSOR)(delayed(treat_report)(
        #     env_vars, report) for report in reports)

    else:
        logger.error("Error while fetching reports")

    print("Waiting for next iteration in", sys_waitime, "seconds...")
    sleep(sys_waitime)
