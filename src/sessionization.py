from datetime import datetime, timedelta


def parse(line):
    line = line.strip()
    fields = line.split(',')

    if len(fields) < 15:
        print("Bad record: {0}").format(line)

    # Get required fields
    ipaddress = fields[0]

    # document count for unique cik, accession, extention
    document = {}
    unique = "_".join([fields[4], fields[5], fields[6]])
    document[unique] = 1
    access_datetime = to_date(fields[1], fields[2])

    # return (ipaddress, ipdate, iptime,cik, accession, extention)
    return {
    "ipaddress" : ipaddress,
    "access_datetime" : access_datetime,
    "cik_accession_extention": document
    }


def process_stream(logfile, summaryfile, inactive_period):
    logs = []
    """ Sessionization web log """
    with open(logfile) as stream:
        header = stream.readline()
        for line in stream:
            row = parse(line)
            exist = False
            i = 0
            length = len(logs)
            for i in range(length):
                log = logs[i]
                if log["ipaddress"] == row["ipaddress"]:
                    # Increment count
                    document = log["cik_accession_extention"]
                    name, value = row["cik_accession_extention"].items()[0]
                    if name in document:
                        document[name] = document[name] + value
                    else:
                        document[name] = value

                    # Session complete
                    start = log["access_datetime"]
                    end = row["access_datetime"]
                    delta_in_sec = (start-end).total_seconds()
                    print(delta_in_sec)

                    if(delta_in_sec == inactive_period):
                        with open(summaryfile, "w+") as summary:
                            dict_length = len(document)
                            new_row = ",".join([log["ipaddress"], start, end, delta_in_sec, dict_length])
                            summary.write(new_row)
                        logs.remove(i)
                        break

                else:
                    logs.append(row)
    if len(logs) != 0:
        i = 0
        log = logs[i]
        log_length = len(logs)
        for i in range(log_length):
            # Increment count
            document = log["cik_accession_extention"]
            name, value = row["cik_accession_extention"].items()[0]
            if name in document:
                document[name] = document[name] + value
            else:
                document[name] = value
            start = log["access_datetime"]
            end = row["access_datetime"]
            delta_in_sec = (start - end).total_seconds()
            print(delta_in_sec)
            with open(summaryfile, "w+") as summary:
                dict_length = len(document)
                new_row = ",".join([log["ipaddress"], start, end, delta_in_sec, dict_length])
                summary.write(new_row)


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

    process_stream(logfile, summaryfile, inactive_period)

