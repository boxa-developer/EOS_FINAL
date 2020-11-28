from ..modules.query import insert, select_many, select_one
from rest_framework.decorators import api_view
from django.http import JsonResponse
from ..modules.config import *
from loguru import logger
import requests


@api_view(['POST'])
def create_tasks_zero_type(request):
    data = request.data
    query_data = select_many("""
        SELECT
            feature_data::json->'geometry',
            polygon_id,
            Box2d(ST_Transform(shape, 4326))
        FROM
            eos.api_polygons
        WHERE
            fermer_id = {}
        ORDER BY 
            polygon_id ASC;
    """.format(data["fid"]))

    for i, (geometry, polygon_id, bounding_box) in enumerate(query_data):
        logger.info(f"Requesting for #{polygon_id}")
        request_data = {
            "type": "mt_stats",
            "params": {
                "bm_type": "NDVI",
                "date_start": data["start_date"],
                "date_end": data["end_date"],
                "geometry": geometry,
                "reference": "ref_datetime",
                "sensors": ["sentinel2l2a"],
                "limit": 500,
                "max_cloud_cover_in_aoi": 60,
                "cloud_masking_level": 3

            }
        }
        import time
        if i % 10 == 0:
            time.sleep(6)
        create_task_request = requests.post(
            url=f"https://gate.eos.com/api/gdw/api?api_key={ATTR['api_key']}",
            headers=HEADERS["CONTENT"],
            json=request_data
        )
        logger.info(f"Response Accepted task_id: {create_task_request.json()['task_id']}!")
        insert("""
            INSERT INTO
                eos.api_tasks (date, polygon_id, task_type, task_status, task_id)
            VALUES 
                ( '{}', {}, {}, '{}', '{}' )
            ON CONFLICT ON CONSTRAINT task_cnt
            DO NOTHING;                 
        """.format((data["start_date"] + '/' + data["end_date"]),
                   polygon_id, 0, 'created',
                   create_task_request.json()["task_id"]))
        logger.success("Successfully added to database!")

    return JsonResponse("DONE!", safe=False)


@api_view(['POST'])
def create_tasks_one_type(request, stop_id):
    query_data = select_many("""
        SELECT
            feature_data::json->'geometry',
            polygon_id,
            Box2d(ST_Transform(shape, 4326))
        FROM
            eos.api_polygons
        WHERE polygon_id>={}
        ORDER BY
            polygon_id ASC;
    """.format(stop_id))

    for i, (geometry, pid, bounding_box) in enumerate(query_data):
        bound_box = [(bounding_box.split("(")[1].split(")")[0].split(",")[i].split(" "))
                     for i in range(len(bounding_box.split("(")[1].split(")")[0].split(",")))]
        bbox = [eval(bound_box[0][0]), eval(bound_box[0][1]), eval(bound_box[1][0]), eval(bound_box[1][1])]
        search_data = select_many("""
            SELECT
                view_id
            FROM
                eos.api_view_id
            WHERE
                polygon_id = {};
        """.format(pid))
        dates = select_many("""
                        SELECT 
                            date 
                        FROM 
                            eos.api_tasks
                        WHERE
                            polygon_id={};                
                    """.format(pid))
        dates = [dates[i][0] for i in range(len(dates))]
        logger.info(f"Found {len(search_data)} views from #{pid}")
        # print(dates)
        for k, view_id in enumerate(search_data):
            if '-'.join(view_id[0].split('/')[4:-1]) not in dates:
                task_data = {
                    "type": 'cl_stats',
                    "params": {
                        "scene_id": view_id[0].replace('/', '-'),
                        "bm_type": "NDVI",
                        "bbox": bbox,
                        "thresholds": [(j / 100, (j + 5) / 100) for j in range(0, 100, 5)],
                        "reference": "ref_datetime"
                    }
                }
                logger.info(f'Requested for {k + 1}/{len(search_data)} create task!')
                print('1')
                task_request = requests.post(
                    url=f"https://gate.eos.com/api/gdw/api?api_key={ATTR['api_key']}",
                    json=task_data,
                    headers=HEADERS['CONTENT']
                )
                print('2-', task_request.json())
                while task_request is None:
                    continue
                logger.info(f'Response accepted task_id: {task_request.json()["task_id"]}')
                insert("""
                    INSERT INTO
                        eos.api_tasks (polygon_id, task_type, task_status, task_id, date)
                    VALUES
                        ({}, {}, '{}', '{}', '{}')
                    ON CONFLICT ON CONSTRAINT task_cnt
                    DO NOTHING;
                """.format(pid, 1, 'created', task_request.json()["task_id"], '-'.join(view_id[0].split('/')[4:-1])))
            else:
                logger.info(f"Request #{k} exists in database!")

        logger.success(f"#{pid} finished!")
    return JsonResponse('ok', safe=False)


@api_view(['GET'])
def task_status(request, task_id):
    status_request = requests.get(
        url=f'https://gate.eos.com/api/gdw/api/{task_id}?api_key={ATTR["api_key"]}'
    )
    return JsonResponse(status_request.json(), safe=False)
