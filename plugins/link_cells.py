from datasette import hookimpl
import markupsafe
import json

COLUMNS = {
    "filer_id": "/filer/{}",
    "contributor_name": "/donor/{}"
}

@hookimpl
def render_cell(value, column, table, database, datasette):
    # Render {"href": "...", "label": "..."} as link
    if column not in COLUMNS:
        return None
    href = COLUMNS[column].format(value)
    return markupsafe.Markup('<a href="{href}">{label}</a>'.format(
        href=href,
        label=value
    ))
