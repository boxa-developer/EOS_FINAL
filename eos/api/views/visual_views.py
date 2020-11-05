from rest_framework.decorators import api_view
from ..modules.query import select_many, select_one
from django.http import JsonResponse
from ..modules.config import *
from ..modules.tools import correct_date
import requests
from django.core.files.storage import default_storage


@api_view(['POST'])
def map_visual(request):
    data = request.data
    crp, crop_type = select_many("""
            SELECT 
                cropper_ref, 
                polygon_type  
            FROM eos_final.api_polygons  
            WHERE polygon_id={};
            """.format(data["pid"]))[0]
    view_id = select_one("""
        SELECT
            view_id
        FROM
            eos_final.api_view_id
        WHERE
            polygon_id={}
    """.format(data["pid"]))

    url = "https://gate.eos.com/api/render/{}/{}/{{z}}/{{x}}/{{y}}?MIN_MAX=0,1&CALIBRATE=1&" \
          "cropper_ref={}&COLORMAP" \
          "={}&api_key={}".format(view_id, COLOR_BANDS[0], crp, COLOR_MAPS[0], ATTR['api_key'])
    send_data = {
        "url": url,
        "id": data["pid"],
        "crop_type": crop_type
    }
    return JsonResponse(send_data, safe=False)


@api_view(['POST'])
def preview_visual(request):
    data = request.data
    query_image = select_many("""
        SELECT 
            path, filename, date
        FROM
            eos_final.api_images
        WHERE
            polygon_id={}
    """.format(data['pid']))
    send_data = None
    for row in query_image:
        if correct_date(data["date"]) >= correct_date(row[2]):
            send_data = {
                'url': f'/{row[0]}/{row[1]}'
            }
            break
        else:
            if correct_date(data["date"]) < correct_date(row[2]):
                continue

    return JsonResponse(send_data, safe=False)
    # if len(query_image) > 0:
    #     if default_storage.exists(f'192.168.2.75:8001/{query_image[0][0]}/{query_image[0][1]}'):
    #         send_data = {
    #             "url": f'/{query_image[0][1]}/{query_image[0][1]}'
    #         }
    #     else:
    #         send_data = {
    #             'url': query_image[0][2]
    #         }
    # else:
    #     send_data = {
    #         "url": "not found"
    #     }
