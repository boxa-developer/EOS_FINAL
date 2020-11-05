from django.core.files.storage import default_storage
from rest_framework.decorators import api_view
from ..modules.query import select_many, insert
from urllib.request import urlretrieve
from django.http import JsonResponse
from ..modules.config import *
from loguru import logger
import os
import requests

# global variables
H = 500


@api_view(['POST'])
def download_images(request, id):
    add_query = '' if id == 0 else f'WHERE polygon_id={id}'
    query_data = select_many("""
                    SELECT
                        Box2d(ST_Transform(shape, 4326)),
                        cropper_ref,
                        polygon_id
                    FROM eos_final.api_polygons {};
                """.format(add_query))
    for i, (bounding_box, cpr, pid) in enumerate(query_data):
        filenames = select_many("""
                SELECT 
                    filename
                FROM
                    eos_final.api_images
                WHERE
                    polygon_id={};
            """.format(pid))
        filenames = [filenames[i][0] for i in range(len(filenames))]
        bounding_box = [(bounding_box.split("(")[1].split(")")[0].split(",")[i].split(" "))
                        for i in range(len(bounding_box.split("(")[1].split(")")[0].split(",")))]
        import math
        aspect_ratio = math.fabs(
            (eval(bounding_box[0][0]) - eval(bounding_box[1][0])) / (
                    eval(bounding_box[0][1]) - eval(bounding_box[1][1])))
        W = int(H * aspect_ratio)

        search_data = select_many("""
                        SELECT
                            view_id
                        FROM
                            eos_final.api_view_id
                        WHERE
                            polygon_id = {};
                    """.format(pid))

        shape_dir = f'download_images/shape{pid}'
        os.makedirs(shape_dir, exist_ok=True)
        logger.info(f'Destination Folder {shape_dir} FOUND: {len(search_data)}\n\n')
        for k, view_id in enumerate(search_data):
            filename = f'image-{pid}-{view_id[0].replace("/", "_")}.png'
            if filename not in filenames:
                url = f"https://render.eosda.com/{view_id[0]}/{COLOR_BANDS[0]}/16" \
                      f"/{bounding_box[0][0]};{bounding_box[1][0]};" \
                      f"4326/{bounding_box[0][1]};{bounding_box[1][1]};4326?COLORMAP" \
                      f"={COLOR_MAPS[0]}&MASK_COLOR=ffffff&MASKING=CLOUD&MIN_MAX=0," \
                      f"1&cropper_ref={cpr}&TILE_SIZE={W},{H}&CALIBRATE=1 "
                try:
                    dst = f'{shape_dir}/{filename}'
                    logger.info(dst)
                    insert("""
                        INSERT INTO
                            eos_final.api_images (polygon_id, path, filename, web_url, date)
                        VALUES
                            ({}, '{}', '{}', '{}', '{}')
                        ON CONFLICT ON CONSTRAINT image_cnt
                        DO NOTHING;
                    """.format(pid, shape_dir, filename, url, '-'.join(view_id[0].split('/')[4:-1])))
                    urlretrieve(url, dst)
                except:
                    logger.error("Error | reported")
                    insert("""
                                INSERT INTO
                                    eos_final.api_images (polygon_id, path, filename, web_url, date)
                                VALUES
                                    ({}, '{}', '{}', '{}', '{}')
                                ON CONFLICT ON CONSTRAINT image_cnt
                                DO NOTHING;
                        """.format(pid, 'ND', 'ED', url, '-'.join(view_id[0].split('/')[4:-1])))
            else:
                logger.info(f"{filename} {k} exists in database!")

    return JsonResponse('DONE!', safe=False)


@api_view(['GET'])
def conflict_images(request):
    bug_images = select_many("""
        SELECT
            *
        FROM
            eos_final.api_images
        WHERE
            filename = 'ED';
    """)
    logger.info(f"Found {len(bug_images)}")
    for data in bug_images:
        filename = data[5].split('.com/')[1].split('/(NIR-RED)')[0].replace('/','_')
        filename = f'image-{data[4]}-{filename}.png'
        path = f'download_images/shape{data[4]}'
        logger.info(path, filename)
        insert("""
            UPDATE
                eos_final.api_images
            SET
                path = '{}', filename = '{}'
        """.format(path, filename))
    query_data = select_many("""
        SELECT
            path, filename, web_url
        FROM
            eos_final.api_images;
    """)
    count = 0
    for i, (path, filename, web_url) in enumerate(query_data):
        if default_storage.exists(os.path.join(path, filename)):
            logger.info('exists')
        else:
            logger.error('not found')
            count += 1
            logger.info(os.path.join(path, filename))
            # urlretrieve(web_url, path+'/'+filename)
            logger.success(f'Downloaded {count} file')

    return JsonResponse("DONE", safe=False)