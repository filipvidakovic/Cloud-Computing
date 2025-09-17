def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    return auth["claims"].get("sub")
