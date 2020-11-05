from ..modules.query import insert, select_many, select_one
from rest_framework.decorators import api_view
from django.http import JsonResponse
from ..modules.config import *
from ..modules.tools import *
from loguru import logger
import requests
import json


@api_view(['POST'])
def collect_temperature_data(request, id):
    data = request.data
    request_date = data["date"]
    query_str = ';' if id == 0 else f'WHERE polygon_id={id};'

    query_data = select_many("""
        SELECT
            feature_data->'geometry',
            polygon_id
        FROM
            eos_final.api_polygons
        {}
    """.format(query_str))

    for i, (geometry, pid) in enumerate(query_data):

        date = select_many("""
                        SELECT 
                            all_data::json->'dates'->0 l, all_data::json->'dates'->-1 f
                        FROM
                            eos_final.api_data
                        WHERE
                            polygon_id={} AND data_type='temp';
                    """.format(pid))
        save_data = {}
        if len(date) > 0:
            last_date = date[0][1]
            first_date = date[0][0]

            t_date, f_date = first_date, last_date
            ignore = False
            if first_date <= request_date <= last_date:
                ignore = True
            elif first_date <= last_date < request_date:
                f_date = last_date
                t_date = request_date
            elif request_date < first_date < last_date:
                f_date = request_date
                t_date = first_date
            if not ignore:
                temp_data = {
                    "geometry": geometry,
                    "start_date": f_date,
                    "end_date": t_date
                }
                logger.info("Requested!")
                temp_request = requests.post(
                    url=f'https://gate.eos.com/api/cz/backend/forecast-history/?api_key={ATTR["api_key"]}',
                    json=temp_data,
                    headers=HEADERS["CONTENT"]
                )

                temp_json = sorted(temp_request.json(), key=lambda k: k['date'])
                save_data = {
                    'dates': [temp_json[i]["date"] for i in range(len(temp_json))],
                    'min': [temp_json[i]["temperature_min"] for i in range(len(temp_json))],
                    'max': [temp_json[i]["temperature_max"] for i in range(len(temp_json))],
                }
                insert("""
                    UPDATE
                        eos_final.api_data
                    SET
                        all_data = jsonb_set(   all_data::jsonb,
                                                array['dates'],
                                                (all_data->'dates')::jsonb || '{}'::jsonb)
                """.format(json.dumps(save_data['dates'])))
                insert("""
                                    UPDATE
                                        eos_final.api_data
                                    SET
                                        all_data = jsonb_set(   all_data::jsonb,
                                                                array['max'],
                                                                (all_data->'max')::jsonb || '{}'::jsonb)
                                """.format(json.dumps(save_data['max'])))
                insert("""
                                    UPDATE
                                        eos_final.api_data
                                    SET
                                        all_data = jsonb_set(   all_data::jsonb,
                                                                array['min'],
                                                                (all_data->'min')::jsonb || '{}'::jsonb)
                                """.format(json.dumps(save_data['min'])))
            else:
                logger.info("All data available in database!")
        else:
            temp_data = {
                "geometry": geometry,
                "start_date": ATTR['date_from'],
                "end_date": request_date
            }

            temp_request = requests.post(
                url=f'https://gate.eos.com/api/cz/backend/forecast-history/?api_key={ATTR["api_key"]}',
                json=temp_data,
                headers=HEADERS["CONTENT"]
            )
            temp_json = sorted(temp_request.json(), key=lambda k: k['date'])

            save_data = {
                'dates': [temp_json[i]["date"] for i in range(len(temp_json))],
                'min': [temp_json[i]["temperature_min"] for i in range(len(temp_json))],
                'max': [temp_json[i]["temperature_max"] for i in range(len(temp_json))],
            }
            insert("""
                    INSERT INTO
                        eos_final.api_data (polygon_id, date, data_type, all_data)
                    VALUES
                        ({}, '{}','temp', '{}')
                    """.format(pid, data["date"], json.dumps(save_data)))
    return JsonResponse('DONE!', safe=False)


@api_view(['POST'])
def collect_veg_data(request, id):
    data = request.data
    request_date = data["date"]
    query_str = ';' if id == 0 else f'AND polygon_id={id};'

    query_data = select_many("""
        SELECT
            task_id,
            polygon_id
        FROM
            eos_final.api_tasks
        WHERE task_type=0 {}
    """.format(query_str))

    for i, (task_id, pid) in enumerate(query_data):

        date = select_many("""
                        SELECT 
                            all_data::json->'dates'->0 l, all_data::json->'dates'->-1 f
                        FROM
                            eos_final.api_data
                        WHERE
                            polygon_id={} AND data_type='veg';
                    """.format(pid))
        save_data = {}
        if len(date) > 0:
            last_date = date[0][1]
            first_date = date[0][0]

            t_date, f_date = first_date, last_date
            ignore = False
            if first_date <= request_date <= last_date:
                ignore = True
            elif first_date <= last_date < request_date:
                f_date = last_date
                t_date = request_date
            elif request_date < first_date < last_date:
                f_date = request_date
                t_date = first_date
            if not ignore:

                logger.info("Requested!")
                vegetation_request = requests.get(url=f"https://gate.eos.com/api/gdw/api/{task_id}?api_key={ATTR['api_key']}")

                temp_json = sorted(vegetation_request.json(), key=lambda k: k['date'])
                save_data = {
                    'dates': [temp_json[i]["date"] for i in range(len(temp_json))],
                    'veg': [temp_json[i]["temperature_min"] for i in range(len(temp_json))],
                }
                insert("""
                    UPDATE
                        eos_final.api_data
                    SET
                        all_data = jsonb_set(   all_data::jsonb,
                                                array['dates'],
                                                (all_data->'dates')::jsonb || '{}'::jsonb)
                """.format(json.dumps(save_data['dates'])))
                insert("""
                                    UPDATE
                                        eos_final.api_data
                                    SET
                                        all_data = jsonb_set(   all_data::jsonb,
                                                                array['veg'],
                                                                (all_data->'veg')::jsonb || '{}'::jsonb)
                                """.format(json.dumps(save_data['veg'])))
            else:
                logger.info("All data available in database!")
        else:
            vegetation_request = requests.get(f'https://gate.eos.com/api/gdw/api/{task_id}?api_key={ATTR["api_key"]}')

            temp_json = sorted(vegetation_request.json()['result'], key=lambda k: k['date'])
            save_data = {
                'dates': [temp_json[i]["date"] for i in range(len(temp_json))],
                'veg': [temp_json[i]["average"] for i in range(len(temp_json))],
            }

            insert("""
                    INSERT INTO
                        eos_final.api_data (polygon_id, date, data_type, all_data)
                    VALUES
                        ({}, '{}','veg', '{}')
                    """.format(pid, data["date"], json.dumps(save_data)))
    return JsonResponse('DONE!', safe=False)


@api_view(['POST'])
def collect_area_data(request, id):
    data = request.data
    query_str = ';' if id == 0 else f'AND polygon_id={id};'

    query_data = select_many("""
        SELECT
            task_id,
            polygon_id,
            date
        FROM
            eos_final.api_tasks
        WHERE task_type=1 {}
    """.format(query_str))

    for i, (task_id, pid, date) in enumerate(query_data):
        dates = select_many("""
            SELECT
                date
            FROM
                eos_final.api_data
            WHERE
                polygon_id = {}
        """.format(pid))
        dates = [dates[i][0] for i in range(len(dates))]
        date = correct_date(date)
        if date not in dates:
            area_request = requests.get(f'https://gate.eos.com/api/gdw/api/{task_id}?api_key={ATTR["api_key"]}')
            try:
                temp_json = area_request.json()["result"]
                save_data = {
                    'area': [temp_json[i]["area"]/10000 for i in range(len(temp_json))],
                }

                insert("""
                        INSERT INTO
                            eos_final.api_data (polygon_id, date, data_type, all_data)
                        VALUES
                            ({}, '{}','area', '{}')
                        """.format(pid, date, json.dumps(save_data)))
            except:
                logger.error(f"pid: {pid} task_id: {task_id} date: {date}")
        else:
            logger.info('exists in database')
    return JsonResponse('DONE!', safe=False)