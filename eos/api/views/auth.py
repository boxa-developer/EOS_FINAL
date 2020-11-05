from rest_framework.decorators import api_view
from django.http.response import JsonResponse


@api_view(['POST'])
def login_form(request):
    data = request.data
    if data['username'] == 'admin' and data['password'] == 'parol12345':
        return JsonResponse({"status": "success"}, safe=False)
    else:
        return JsonResponse({"status": "denied"}, safe=False)
