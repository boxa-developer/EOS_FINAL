from rest_framework.decorators import api_view
from ..modules.query import select_many, select_one
from ..modules.config import ATTR, HEADERS
from django.http import JsonResponse
import requests
import json


@api_view(['POST'])
def get_geojson(request):
    data = request.data
    print(data["crop_type"])
    if data["crop_type"] != 0:
        add_query = f" AND polygon_type={data['crop_type']}"
    else:
        add_query = ""
    send_data = select_one("""SELECT
                        json_build_object('type', 'FeatureCollection', 'features', 
                            json_agg(ST_AsGeoJson(ST_Transform(shape, 4326))::jsonb || jsonb_build_object('properties',
                             jsonb_build_object('id', feature_data->'id', 'pid', polygon_id) ))
                        ) as res
                    FROM eos.api_polygons 
                    WHERE fermer_id={} {};""".format(data["fermer_id"], add_query))
    return JsonResponse(send_data, safe=False)


@api_view(['POST'])
def area_data(request):
    data = request.data
    query_data = select_one("""
        SELECT
            all_data
        FROM
            eos.api_data
        WHERE
            data_type = 'area' AND polygon_id={} AND date='{}';
    """.format(data['pid'], data['date']))
    send_data = {
        "area": {str(i): json.loads(query_data)["area"][::-1][i] for i in range(len(json.loads(query_data)['area']))}
    }

    return JsonResponse(send_data, safe=False)


@api_view(['POST'])
def chart_data(request):
    data = request.data
    temp_data = select_many("""
        SELECT
            all_data->'dates',
            all_data->'min',
            all_data->'max'
        FROM
            eos.api_data
        WHERE
            data_type='temp' AND polygon_id={}
    """.format(data["pid"]))

    veg_data = select_many("""
        SELECT
            all_data->'veg',
            all_data->'dates'
        FROM
            eos.api_data
        WHERE
            data_type='veg' AND polygon_id={}
    """.format(data["pid"]))

    veg_json = {
        'veg': json.loads(veg_data[0][0]),
        'dates': json.loads(veg_data[0][1])
    }

    temp_json = {
        'dates': json.loads(temp_data[0][0]),
        'min': json.loads(temp_data[0][1]),
        'max': json.loads(temp_data[0][2])
    }

    res = {
        'x': [],
        'max': [],
        'min': [],
        'veg': []
    }
    # veg_json = sorted(veg_json, key=lambda k: k['dates'], reverse=True)
    # temp_json = sorted(temp_json, key=lambda k: k['dates'], reverse=True)
    tmp_veg_json = {
        'veg': [],
        'dates': []
    }
    for i in range(1, len(veg_json['veg'])):
        if veg_json['veg'][i] > 0.11 or veg_json['veg'][i-1]-veg_json['veg'][i]< 0.3:
            tmp_veg_json['veg'].append(veg_json['veg'][i])
            tmp_veg_json['dates'].append(veg_json['dates'][i])
    veg_json = tmp_veg_json
    for i in range(len(temp_json["dates"])):
        tmp = -1
        for j in range(len(veg_json["dates"])):
            if veg_json['dates'][j] == temp_json['dates'][i]:
                # if (sorted_vr[j - 1]['average'] - sorted_vr[j]['average'] > 0.13) or sorted_vr[j]['average'] < 0.1:
                # if veg_json['veg'][j - 1] - veg_json['veg'][j] > 0.11:
                #     tmp = -1
                # else:
                tmp = veg_json['veg'][j]
                # break
        if tmp > -1:
            res['x'].append(temp_json['dates'][i])
            res['max'].append(temp_json['max'][i])
            res['min'].append(temp_json['min'][i])
            res['veg'].append(tmp)

    return JsonResponse(res, safe=False)


@api_view(['POST'])
def area_stat(request):
    data = request.data
    query_data = select_many("""
        SELECT
            task_id
        FROM
            eos.api_tasks
        WHERE
            task_type=1 
        AND 
            polygon_id={}
        AND
            date='{}'    
    """.format(data["pid"], data['date']))
    # return JsonResponse(query_data, safe=False)
    if len(query_data) > 0:
        task_id = query_data[0][0]
        stat_request = requests.get(
            url=f"https://gdw.eos.com/api/{task_id}?api_key={ATTR['api_key']}"
        )
        return JsonResponse(stat_request.json(), safe=False)
