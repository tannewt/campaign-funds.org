from datasette import hookimpl
from datasette.version import __version__
import pathlib
import time

@hookimpl
def extra_template_vars(template, database, table, columns, view_name, request, datasette):
    if view_name != "page":
        return {}
    db = datasette.get_database("raw")
    p = pathlib.Path(db.path)
    return {
        "db_updated": time.strftime("%B %d, %Y", time.localtime(p.stat().st_mtime)),
        "datasette_version": __version__
    }
