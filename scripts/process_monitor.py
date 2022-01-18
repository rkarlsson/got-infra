#!/usr/bin/python3

import os
import psutil
import subprocess
from datetime import datetime, time
import logging

GOT_BIN="/GoT/build/bin/"
AERONMD_HOME="/GoT/submodules/aeron/cppbuild/binaries/"
my_hostname = os.uname()[1]

svc_lst = [
    {
        "hostname":"tokyo1",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_md_binance", "-m", "-E", "PROD", "-a", "-r", "AD"],
        "capture_process":"no"
    },
    {
        "hostname":"tokyo1",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_md_binance", "-m", "-E", "PROD", "-a", "-r", "EM"],
        "capture_process":"no"
    },
    {
        "hostname":"tokyo1",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_md_binance", "-m", "-E", "PROD", "-a", "-r", "NZ"],
        "capture_process":"no"
    },
    {
        "hostname":"tokyo1",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_oe_binance", "-l", "Tokyo", "-E", "PROD"],
        "capture_process":"no"
    },
    {
        "hostname":"tokyo1",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_risk", "-E", "PROD"],
        "capture_process":"no"
    },
    {
        "hostname":"tokyo1",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_monitor", "-E", "PROD", "-e", "i"],
        "capture_process":"no"
    },
    {
        "hostname":"tokyo3",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_md_binance", "-c", "-E", "PROD", "-a", "-r", "AD"],
        "capture_process":"yes"
    },
    {
        "hostname":"tokyo3",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_md_binance", "-c", "-E", "PROD", "-a", "-r", "EM"],
        "capture_process":"yes"
    },
    {
        "hostname":"tokyo3",
        "dir_path":GOT_BIN,
        "exec_cmd":[GOT_BIN + "svc_md_binance", "-c", "-E", "PROD", "-a", "-r", "NZ"],
        "capture_process":"yes"
    }
]

#######################################################################
def is_time_between(begin_time, end_time, check_time=None):
    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.utcnow().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else: # crosses midnight
        return check_time >= begin_time or check_time <= end_time


#######################################################################
def check_media_driver():
    for process in psutil.process_iter():
        cmdline = process.cmdline()
        if (any(s.endswith("aeronmd") for s in cmdline)):
            logging.info("Found aeronmd, continuing...")
            return

    logging.info("Found no aeronmd running - will attempt to start it here")
    my_env = {**os.environ, 'USER': 'ubuntu'}
    subprocess.Popen(
        AERONMD_HOME + "aeronmd",
        stdout=None, 
        stderr=None,
        close_fds=True,
        env=my_env,
        preexec_fn=os.setpgrp )

#######################################################################
def get_oldest_pid_for_service(service_map):
    pid_list = []
    for process in psutil.process_iter():
        cmdline = process.cmdline()
        svc_list = service_map["exec_cmd"]
        matches = [c for c in svc_list if c in cmdline]
        if(len(svc_list) == len(matches)):
            pid_list.append(process.pid)

    # We expect 2 processes
    if(len(pid_list) == 1):
        return(0)

    elif(len(pid_list) == 2):
        if (psutil.Process(pid_list[0]).create_time() < psutil.Process(pid_list[1]).create_time()):
            return(pid_list[0])
        else:
            return(pid_list[1])
    else:
        return(0)

#######################################################################
def get_pid_for_service(service_map):
    for process in psutil.process_iter():
        cmdline = process.cmdline()
        svc_list = service_map["exec_cmd"]
        matches = [c for c in svc_list if c in cmdline]
        if(len(svc_list) == len(matches)):
            return(process.pid)
    return(0)

#######################################################################
def start_next_days_captures():
    extra_commandline_options = ["-o", "1"] # This will start with next days filename
    for service_entry in svc_lst:
        if service_entry["hostname"] == my_hostname:
            if(service_entry["capture_process"].lower() == "yes"):
                svc_cmd = service_entry["exec_cmd"]
                executing_cmd = svc_cmd + extra_commandline_options
                logging.info("Starting next days capture: " + str(" ".join(map(str, executing_cmd))))
                my_env = {**os.environ, 'USER': 'ubuntu'}
                subprocess.Popen(
                    executing_cmd,
                    stdout=None, 
                    stderr=None,
                    close_fds=True,
                    env=my_env,
                    preexec_fn=os.setpgrp )

#######################################################################
def stop_previous_days_captures():
    for service_entry in svc_lst:
        if service_entry["hostname"] == my_hostname:
            if(service_entry["capture_process"].lower() == "yes"):
                svc_cmd = service_entry["exec_cmd"]
                logging.info("Stopping previous days capture for: " + str(" ".join(map(str, svc_cmd))))
                pid = get_oldest_pid_for_service(service_entry)
                if(pid != 0):
                    logging.info("Found PID: " + str(pid))
                    psutil.Process(pid).terminate()

#######################################################################
def monitor_app(service_entry):
        ret_pid = get_pid_for_service(service_entry)
        if(ret_pid == 0): # It will never be a 0 PID process, so safe to assume that first element shouldn't be it..
            svc_cmd = service_entry["exec_cmd"]
            logging.info("App: \"" + str(" ".join(map(str, svc_cmd))) + "\" - NOT RUNNING - STARTING")
            my_env = {**os.environ, 'USER': 'ubuntu'}
            subprocess.Popen(
                svc_cmd,
                stdout=None, 
                stderr=None,
                close_fds=True,
                env=my_env,
                preexec_fn=os.setpgrp )

#######################################################################
def main():
    logging.basicConfig(filename='/home/ubuntu/process_monitor.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

    # # Check that the aeron media driver is running accordingly
    # check_media_driver()

    # # Monitor all services in list
    for service_entry in svc_lst:
        if service_entry["hostname"] == my_hostname:
            monitor_app(service_entry)

    if is_time_between(time(23,40), time(23,41)):
        logging.info("Time is between 23:40 and 23:41 - starting next days captures")
        start_next_days_captures()

    if is_time_between(time(00,1), time(00,2)):
        logging.info("Time is between 00:01 and 00:02 - stopping previous days captures")
        stop_previous_days_captures()

if __name__ == "__main__":
    main()
