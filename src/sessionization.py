from datetime import datetime, timedelta


def parse(line):
    line = line.strip()
    fields = line.split(',')

    if len(fields) < 15:
        print("Bad record: {0}").format(line)

    # Get required fields
    ipaddress = fields[0]

    # document name and unique cik, accession, extention
    document = "_".join([fields[4], fields[5], fields[6]])
    access_datetime = to_date(fields[1], fields[2])

    return {
    "ipaddress" : ipaddress,
    "access_datetime" : access_datetime,
    "cik_accession_extention": document
    }

def process_stream_2(logfile, summaryfile, inactive_period):
    sessions = []
    users = {}
    begin = True
    """ Sessionization web log """
    with open(logfile) as stream:
        header = stream.readline()
        for line in stream:
            row = parse(line)
            process_time = row["access_datetime"]

            # Add user and the document it accessed to a session
            sessions = add_user_to_session(sessions, process_time, row)

            # Identify inactive users and write to file
            sessions = inactive_users(sessions, process_time, inactive_period)

def inactive_users(sessions, process_time, inactive_period):
    length = len(sessions)
    for i in range(length):
        session_datetime, users = sessions[i].items()[0]
        delta = (process_time - session_datetime).total_seconds()
        if delta == inactive_period:
            #j = i
            #for j in range(j, length):
                print("remains unsolved here when trying to identify inactive users")
                print("lack of time to complete this exercise due to hectic work schedule")
                print("May be I will try to participate in Insight program another time!!!")
    return sessions

def add_user_to_session(sessions, process_time, row):
    # Add user and the document it accessed to a session
    # session = 1 second in a day
    # each session contains users dictionary and each user contians documents as dictionary
    sessions, users = getSession(sessions, process_time)
    if row["ipaddress"] in users:
        # add document
        user = users.get(row["ipaddress"])
        document = row["cik_accession_extention"]
        if document not in user:
            user[document] = 1
        users[row["ipaddress"]] = user
    else:
        # add user and document
        document = row["cik_accession_extention"]
        users[row["ipaddress"]] = {document: 1}
    sessions = updateSession(sessions, process_time, users)
    print("===============================================")
    print("===============================================")

    for i in range(len(sessions)):
        pretty(sessions[i])

    return sessions

def pretty(d, indent=0):
   for key, value in d.items():
      print('\t' * indent + str(key))
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print('\t' * (indent+1) + str(value))

def getSession(sessions, current):
    """ Return users in a session"""
    for session in sessions:
        session, users = session.items()[0]
        if session == current:
            return sessions, users
    users = {}
    session = {current:users}
    sessions.append(session)
    return sessions, users

def updateSession(sessions, process_time, users):
    length = len(sessions)
    for i in range(length):
        session, users = sessions[i].items()[0]
        if session == process_time:
            dict1 = {session:users}
            sessions[i] = dict1
            return sessions
    users = {}
    dict1 = {session: users}
    session.append(dict1)
    return sessions

def to_date(date_str, time_str):
    """ Converts date and time string to datetime data type """
    combine = " ".join([date_str, time_str])
    dt_format = "%Y-%m-%d %H:%M:%S"
    dt = datetime.strptime(combine, dt_format)
    return dt


if __name__ == '__main__':
    logfile = '../input/log.csv'
    summaryfile = '../output/sessionization.txt'
    inactivefile = "../input/inactivity_period.txt"

    with open(inactivefile) as f_inactive:
        inactive_period = int(f_inactive.readline())

    process_stream_2(logfile, summaryfile, inactive_period)

