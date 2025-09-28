def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None