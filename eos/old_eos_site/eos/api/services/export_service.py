from ..modules.query import insert, select_many
from rest_framework.decorators import api_view
from django.http import JsonResponse
from geojson_rewind import rewind
from ..modules.config import *
from loguru import logger
import requests
import json

'''
fermer_id integer NOT NULL,
    feature_data jsonb,
    shape geometry,
    cropper_ref text COLLATE pg_catalog."default",
    polygon_type bytea,
    polygon_id integer,
    id integer DEFAULT nextval('eos_final.id'::regclass),
    collection jsonb
    

'''


@api_view(["POST"])
def export_polygons(request):
    data = request.data
    export_data = select_many("""
        SELECT 
            id, fermer_id, ST_AsGeoJSON(ST_Transform(way, 4326))::json,
            crop_type, way, ST_Area(way)
        FROM agromonitoring.polygons2
        WHERE fermer_id = {}; 
    """.format(data["fid"]))

    ids = []
    for row in export_data:
        geometry = row[2]
        raw_data = {
            "action": "create",
            "type": "Feature",
            "message": "EOS project by Fizmasoft",
            "properties": {
                "name": "Hi from PyDeveloper"
            },
            "geometry": geometry,

        }

        logger.info("Requested for creating feature... ")
        raw_data['geometry'] = rewind(raw_data['geometry'])
        create_feature = requests.post(
            url='https://vt.eos.com/api/data/feature/',
            json=raw_data,
            headers=HEADERS["AUTH"]
        )
        ids.append(create_feature.json()['id'])
        logger.info(f"Request Accepted #{create_feature.json()['id']}")

    logger.info("Exporting Shapes to Database")
    for i, row in enumerate(export_data):
        pid, fid, geometry, ptype, sp, ar = row
        get_feature = requests.get(
            url=f'https://vt.eos.com/api/data/feature/{ids[i]}',
            headers=HEADERS["AUTH"]
        )
        logger.info("Setting up cropper_ref")
        send_data = {
            "type": "Feature",
            "geometry": geometry
        }
        cropper_request = requests.post(
            url=f"https://gate.eos.com/api/render/cropper/?api_key={ATTR['api_key']}",
            json=send_data
        )
        cpr = cropper_request.json()["cropper_ref"]
        logger.info(f"CPR: {cpr}!")
        insert("""
            INSERT INTO eos_final.api_polygons
                (polygon_id, fermer_id, feature_data,
                polygon_type, shape, area, cropper_ref)
            VALUES
                ({}, {}, '{}', {}, '{}', {}, '{}')
            ON CONFLICT ON CONSTRAINT polygons_cnt
            DO NOTHING;
        """.format(pid, fid, json.dumps(get_feature.json()), ptype, sp, ar, cpr))
        logger.info(f"#{pid} inserted successfully!")

    return JsonResponse("DONE!", safe=False)


@api_view(['POST'])
def explore_views(request):
    data = request.data
    query_data = select_many("""
            SELECT
                feature_data::json->'geometry',
                polygon_id
            FROM
                eos_final.api_polygons
            ORDER BY 
                polygon_id ASC;
        """)

    for i, (geometry, pid) in enumerate(query_data):
        get_views = select_many("""
            SELECT
                view_id
            FROM eos_final.api_view_id
            WHERE polygon_id={}
            ORDER BY split_part(view_id, '/', 5), split_part(view_id, '/', 6), split_part(view_id, '/', 7) DESC   
        """.format(pid))


        search_data = {
            "search": {
                "satellites": [SATELLITES[0]],
                "date": {
                    "from": data["start_date"],
                    "to": data["end_date"]
                },
                "shape": geometry,
            },
            "limit": 500,
            "fields": ["date"]
        }
        logger.info(f"Requesting for #{pid} polygon")
        search_request = requests.post(
            url=f'https://gate.eos.com/api/lms/search/v2?api_key={ATTR["api_key"]}',
            json=search_data,
            headers=HEADERS["CONTENT"]
        )
        
        logger.info(f"Found {search_request.json()['meta']['found']} results ...")
        count = 0
        for item in search_request.json()["results"]:
            count += 1
            insert("""
                INSERT INTO
                    eos_final.api_view_id (polygon_id, view_id)
                VALUES
                    ({}, '{}')
                ON CONFLICT ON CONSTRAINT view_id_cnt
                DO NOTHING;
            """.format(pid, item["view_id"]))
            logger.info(f"Inserted {count} from {search_request.json()['meta']['found']}")
        logger.success(f"#{pid} finished!")

    return JsonResponse('DONE!', safe=False)
