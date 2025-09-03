def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})

    # REST API + Cognito authorizer
    if "claims" in auth:
        return auth["claims"].get("sub")

    # HTTP API + JWT authorizer
    if "jwt" in auth and "claims" in auth["jwt"]:
        return auth["jwt"]["claims"].get("sub")

    return None