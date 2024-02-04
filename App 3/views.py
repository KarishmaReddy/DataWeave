# views.py
from django.http import JsonResponse

def get_score(request, account_id):
    try:
        # Connect to Oracle database
        with connection.cursor() as cursor:
            # Query to calculate the score (example)
            cursor.execute("SELECT COUNT(*) FROM crawled_data WHERE account_code = :account_code", {'account_code': account_code})
            result = cursor.fetchone()[0]

        return JsonResponse({'account_code': account_code, 'score': result})

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
