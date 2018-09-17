from urllib.parse import urlparse

from quart import current_app as app, request, jsonify


def filter_referrers():
    filters = app.config.get('REFERRERS_FILTER')
    if not filters:
        return None
    referrer = request.referrer
    if referrer:
        parsed = urlparse(referrer)
        for filter in filters:
            if parsed.hostname.endswith(filter):
                return None
    return jsonify({
            'ok': False,
            'error': 'Unauthorized',
    }), 403
