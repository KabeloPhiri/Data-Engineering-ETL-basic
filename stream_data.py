import sql_server_stream

def stream_data():
    def get_data():
        """
        Input: None
        Pull data through API request
        Returns:
            json: data from API
        """
        print("Data is streaming")
        import requests
        import json

        res = requests.get("https://randomuser.me/api/")
        return res.json()



    def format_data(res):
        """
        Input: json
        Formats data from json
        Returns:
            list: formatted data
        """
        
        print("Data is formatting")

        id = []
        first_names= []
        last_names = []
        locations = []
        emails = []
        dobs = []
        reg_dates = []
        reg_ages = []
        cell_numbers = []
        for key, value in res['results'][0].items():
            print(key, ":", value)


            if str(key).lower() == 'name':
                print(key)
                first_names.append(res['results'][0]['name']["first"])
                last_names.append(res['results'][0]['name']["last"])


            elif str(key).lower() == 'location':
                loc = ''
                for key,value in res['results'][0]['location'].items():

                    if isinstance(value, dict):
                        for key2, value2 in value.items():
                            loc = loc + ','+ str(value2)
                    else:
                        loc = loc +  ',' +str(value)

                locations.append(loc)
                print(locations)

            elif str(key).lower() == 'email':
                emails.append(res['results'][0]['email'])
                print(emails)

            elif str(key).lower() == 'dob':
                dobs.append(res['results'][0]['dob']['date'])

            elif str(key).lower() == 'registered':
                reg_dates.append(res['results'][0]['registered']['date'])
                reg_ages.append(res['results'][0]['registered']['age'])

            elif str(key).lower() == 'cell':
                cell_numbers.append(res['results'][0]['cell'])

            elif str(key).lower() == 'id':
                if res['results'][0]['id'] is not None:
                    id.append(res['results'][0]['id']['value'])

        #print(first_names, last_names, locations, emails, dobs, reg_dates, reg_ages, cell_numbers, id)
        return first_names, last_names, locations, emails, dobs, reg_dates, reg_ages, cell_numbers, id


    import json
    from kafka import KafkaProducer
    import time
    import logging

    res = get_data()
    res = format_data(res)
    curr_time = time.time()


 

    table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'kp01' AND TABLE_NAME = 'users')
    BEGIN
        CREATE TABLE kp01.users (
            [First Name] NVARCHAR(50),
            [Last Name] NVARCHAR(50),
            [Location] NVARCHAR(MAX),
            [Email] NVARCHAR(100),
            [DOB] DATETIME,
            [Registration Date] DATETIME,
            [Registration Age] INT,
            [Cell Number] NVARCHAR(20),
            [ID] NVARCHAR(50)
        )
    END
    """
    

    sql_server_stream.create_table(table_query,cnxn =  sql_server_stream.sql_connector() )
    while True:
        if time.time() > curr_time + 180:
            break 
        try:
            datas = format_data(get_data())


            insert_statement = """
            INSERT INTO kp01.users ([First Name], [Last Name], [Location], [Email], [DOB], [Registration Date], [Registration Age], [Cell Number], [ID])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            sql_server_stream.insert_data(datas,insert_statement, sql_server_stream.sql_connector())

            #producer.send("user_created", json.dumps(datas).encode('utf-8')) -- This is for Kafka but will ]
            # be used in the next project case where distributed streaming is needed.
            time.sleep(1)

        except Exception as e:
            print(e)
            logging.error(f"An error occured:{e}")

            continue

    return "Data has been streamed"


stream_data()




