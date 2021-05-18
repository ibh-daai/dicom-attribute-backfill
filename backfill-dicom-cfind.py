# Updates a bespoke PostgreSQL database with additional patient/study attributes via DICOM C-FIND
# Search for "TODO" and replace with the approperiate values
# The following attributes are radomized/scrambled by the CTP anonymization pipeline. So this script attempts to get
# the new random values by performing a C-FIND against PACS using the anonymized study UID
# - Patient Name
# - Study date/time

import psycopg2.extras, datetime, time, traceback
from pydicom import Dataset
from pynetdicom import AE, debug_logger

StudyRoot = "1.2.840.10008.5.1.4.1.2.2.1"
PatientRoot= "1.2.840.10008.5.1.4.1.2.1.1"
#debug_logger()

def associate():
    assoc = ae.associate(ae_title="TODO_called_AET", addr="TODO_hostname", port=11112)
    if not assoc.is_established:
        return False
    return assoc

if __name__ == '__main__':
    # Connect to PG SQL
    pgconn = psycopg2.connect("host='TODO_pg_host' dbname='TODO_pg_db' user='TODO_pg_user' password='TODO_pg_password' client_encoding='UTF8'")
    pgconn.autocommit = True
    pgsql = pgconn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    pgsql.execute("SELECT study_id, patient_id, uid, accession FROM study WHERE study_ts IS NULL ORDER BY study_id ")
    rows = pgsql.fetchall()
    total = len(rows)

    ae = AE(ae_title="TODO_calling_AET")
    ae.add_requested_context(StudyRoot)  # study root

    i = 0;
    print(datetime.datetime.now(), flush=True)
    print("\n\n")
    try:
        # DICOM AE connections
        assoc = associate()
        if assoc == False:
            print("Could not establish association - failed on startup")
            print(datetime.datetime.now())
            exit()

        # Now loop through the studies, perform the C-FIND and update the DB
        for row in rows:
            i += 1
            if ((i % 100) == 0):
                print("%.2f percent complete" % (i * 100 / total), flush=True)

            # Prepare to do the C-FIND
            id = row['study_id']
            #uid = row['uid']
            pid = row['patient_id']
            accession = row['accession']

            query = Dataset()
            query.AccessionNumber = accession
            query.StudyInstanceUID = ''
            query.StudyDate = ''
            query.StudyTime = ''
            query.PatientName = ''
            query.QueryRetrieveLevel = 'STUDY'

            patientName = ''
            studyDate = ''
            studyTime = ''
            response = None
            try:
                response = assoc.send_c_find(query, StudyRoot)
            except:
                assoc = associate()
                if assoc == False:
                    print("Could not establish association. Processed {} of {} rows.".format(i, total))
                    print(datetime.datetime.now())
                    exit()
                # re-try c-find request
                response = assoc.send_c_find(query, StudyRoot)
            #print("============= " + accession)

            for (status, data) in response:
                if( not 'Status' in status):
                    print("Problem! Not status returned for accession " + accession, flush=True)

                elif (status.Status == 0xff00 or status.Status == 0xff01):
                    #print(data)
                    uid = str(data.StudyInstanceUID)
                    patientName = str(data.PatientName)
                    studyDate = str(data.StudyDate)
                    studyTime = str(data.StudyTime)
                    break

                elif (status.Status == 0x0000):
                    # no-op - it is the end of the response
                    continue

                else:  # indicates a problem
                    print('ERROR!!! Status: 0x{0:04X}'.format(status.Status), flush=True)
            # end for loop (through DICOM results)

            # Error check
            if( not patientName or not studyDate or not studyTime):
                print("No results returned for study with accession = " + accession, flush=True)
                continue

            # Fix formatting and update the DB
            studyDate = studyDate[0:4] + '-' + studyDate[4:6] + '-' + studyDate[6:8]
            studyTime = studyTime[0:2] + ':' + studyTime[2:4] + ':' + studyTime[4:6]
            #print(patientName + " => " + studyDate + ' ' + studyTime)
            sql = f"""UPDATE study SET uid='{uid}', study_ts='{studyDate} {studyTime}' WHERE study_id={id}; 
                      UPDATE patient SET name='{patientName}' WHERE patient_id={pid};"""
            #print(sql)
            pgsql.execute(sql)
            time.sleep(1) # Allow the server some breathing room
        # end for loop (through DB rows)
    except:
        print("Caught exception. Processed {} of {} rows.".format(i, total))
        traceback.print_exc()

    print(datetime.datetime.now(), flush=True)
    assoc.release()
