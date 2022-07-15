from wwo_hist import retrieve_hist_data


#### Set working directory to store output csv file(s)

import os
os.chdir("C:/Users/Hp Support/Videos/03 - Cursos/05 - Herramientas computacionales/Clase 3 - Scraping Python/WorldWeatherOnline-master")


#### Example code

frequency=12
start_date = '01-JAN-2015'
end_date = '31-DEC-2015'
api_key = '535a0388efce401796922753220807'
location_list = ['20637', '20653', '20688', '20735', '20876', '21040', '21042', 
'21157', '21202', '21220', '21412', '21502', '21601', '21638', 
'21639', '21643', '21651', '21709', '21749', '21801', '21811', 
'21853', '21902']

hist_weather_data = retrieve_hist_data(api_key,
                                location_list,
                                start_date,
                                end_date,
                                frequency,
                                location_label = False,
                                export_csv = True,
                                store_df = True)
