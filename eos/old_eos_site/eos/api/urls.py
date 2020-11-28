from django.urls import path
from .services.task_service import create_tasks_zero_type, create_tasks_one_type
from .services.export_service import export_polygons, explore_views
from .services.images_service import download_images, conflict_images
from .services.data_service import collect_temperature_data, collect_veg_data, collect_area_data
from .views.data_views import get_geojson, chart_data, area_stat, area_data
from .views.visual_views import preview_visual, map_visual
from .views.auth import login_form
from django.shortcuts import render


def admin_panel(request):
    return render(request, 'admin-panel.html')


urlpatterns = [
                  path('export_features', export_polygons),
                  path('zero_task', create_tasks_zero_type),
                  path('one_task/<int:stop_id>', create_tasks_one_type),
                  path('explore_views', explore_views),
                  path('download_images/<int:id>', download_images),
                  path('check_conflict', conflict_images),
                  path('collect_temp/<int:id>', collect_temperature_data),
                  path('collect_veg/<int:id>', collect_veg_data),
                  path('collect_area/<int:id>', collect_area_data)
              ] + \
              [
                  path('area_data', area_data),  # pid date
                  path('download_visual', map_visual),  # pid
                  path('chart_data', chart_data),  # pid
                  path('download_ndvi', preview_visual),  # pid date
                  path('get_geojson', get_geojson),
              ] + \
              [
                  path('', admin_panel),
                  path('auth', login_form)
              ]
