from datasette import hookimpl
from functools import wraps


@hookimpl
def asgi_wrapper(datasette):
    def wrap_with_index_override(app):
        @wraps(app)
        async def override_index(scope, receive, send):
            if scope["type"] == "http":
                # Move / to /index to match /pages/index.html
                if scope["path"] == "/" and scope["raw_path"] == b"/":
                    scope["path"] = "/index"
                    scope["raw_path"] = b"/index"
                elif scope["path"] == "/robots.txt" and scope["raw_path"] == b"/robots.txt":
                    scope["path"] = "/robots"
                    scope["raw_path"] = b"robots"
                else:
                    # Move the normal index to /db/
                    if scope["path"].startswith("/db"):
                        scope["path"] = scope["path"][3:]
                    if scope["raw_path"].startswith(b"/db"):
                        scope["raw_path"] = scope["raw_path"][3:]

            await app(scope, receive, send)
        return override_index
    return wrap_with_index_override
