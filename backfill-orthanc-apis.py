# Updates a bespoke PostgreSQL database with additional patient/study attributes retrieved via Orthanc's REST API
# Search for "TODO" and replace with the approperiate values
# The following attributes are radomized/scrambled by the CTP anonymization pipeline. So this script attempts to get
# the new random values by performing queries against Orthanc using the anonymized study UID
# - Study instance UID
# - Study date/time

import http.client, json, datetime, psycopg2.extras, time
from codecs import encode

orthancHeaders = {'Authorization': 'Basic b3J0aGFuYzpvcnRoYW5j'} # Default orthanc/orthanc login
orthancHost = "TODO_orthanc_host"
orthancPort = 8042

def listAllStudies(conn):
    conn.request("GET", '/studies', '', orthancHeaders)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))

def getStudyInstancesTags(id, conn):
    conn.request("GET", "/studies/" + id + "/instances-tags", '', orthancHeaders)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))

if __name__ == '__main__':
    # Connect to PG SQL
    pgconn = psycopg2.connect("host='TODO_pg_host' dbname='TODO_pg_db' user='TODO_pg_user' password='TODO_pg_password' client_encoding='UTF8'")
    pgconn.autocommit = True
    pgsql = pgconn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    orthancStudyIds = listAllStudies(http.client.HTTPConnection(orthancHost, orthancPort))

    i = 0
    total = len(orthancStudyIds)
    print(datetime.datetime.now(), flush=True)

    for orthancStudyId in orthancStudyIds:
        #print("============================================")
        i += 1
        if ((i % 1000) == 0):
            print("%.2f percent complete" % (i * 100 / total), flush=True)
            time.sleep(5) # Give Orthanc room to breath
        #if( i > 2000 ): break

        studyData = getStudyInstancesTags(orthancStudyId, http.client.HTTPConnection(orthancHost, orthancPort))

        accession = None
        studyUid = None

        for seriesId in studyData:
            seriesData = studyData[seriesId]
            studyUid = seriesData["0020,000d"]["Value"]
            accession = seriesData["0008,0050"]["Value"]

            sql = f"UPDATE study SET uid='{studyUid}', study_ts='{studyDate} {studyTime}' WHERE accession='{accession}'"
            #print(sql)
            pgsql.execute(sql)

    print(datetime.datetime.now())
