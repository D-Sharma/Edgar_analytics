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
    with open(logfile, "r") as stream, open(summaryfile, "a") as session_output:
        header = stream.readline()
        for line in stream:
            row = parse(line)
            process_time = row["access_datetime"]

            # Add user and the document it accessed to a session
            sessions = add_user_to_session(sessions, process_time, row)

            # Identify inactive users and write to file
            sessions = inactive_users(sessions, process_time, inactive_period, session_output)


def inactive_users(sessions, current_time, inactive_period, session_output):
    sessions_length = len(sessions)
    # look at older sessions to identify inactive users and write it to file.
    for i in range(sessions_length):
        session_datetime, users = sessions[i].items()[0]
        # For each user in session older than inactive period, search whether use has been inactive
        # Look for inactivity users in the sessions after the current session
        for item in users.items():
            user_ipaddress, documents = item
            # Last access datetime for this user
            last_access_datetime = session_datetime
            count = len(documents)
            delta = (current_time - session_datetime).total_seconds()
            # For each user, get last access datetime if exist in other later sessions.
            j = i + 1
            end_session = dict()
            end_session[session_datetime] = user_ipaddress
            has_sessions = False
            for j in range(j, sessions_length):
                # Check whether users exist in this session
                current_session_datetime, current_users = sessions[j].items()[0]
                if user_ipaddress in current_users.keys():
                    has_sessions = True
                    end_session[current_session_datetime] = user_ipaddress
                    last_documents = current_users[user_ipaddress]
                    last_access_datetime = current_session_datetime
                    count = count + len(last_documents)
                    delta = (last_access_datetime - session_datetime).total_seconds()
                    if delta == inactive_period:
                        # User has been inactive, hence write the session to file
                        new_line = "\n"
                        fields = [user_ipaddress, str(session_datetime), str(last_access_datetime), str(delta), str(count), new_line]
                        row = ",".join(fields)
                        write_session(session_output, fields)
                        sessions = remove_user(sessions, end_session)
            if not has_sessions:
                if delta == inactive_period:
                    new_line = "\n"
                    fields = [user_ipaddress, str(session_datetime), str(last_access_datetime), str(delta), str(count),
                            new_line]
                    write_session(session_output, fields)
                    sessions = remove_user(sessions, end_session)
    return sessions


def write_session(session_output, fields):
    # User has been inactive, hence write the session to file
    row = ",".join(fields)
    session_output.write(row)

def remove_user(sessions, end_sessions):
    for item in end_sessions.items():
        session_time, user = item
        for i in range(len(sessions)):
            session, users = sessions[i].items()[0]
            if session == session_time and user in users:
                users.pop(user, None)
                sessions[i] = {session:users}
                break
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

