import os

from slugify import slugify

from ckanapi import RemoteCKAN

from spacerat.model import Question

ckan_url = os.environ.get("CKAN_URL", "https://data.wprdc.org")

ckan = RemoteCKAN(ckan_url)


def get_questions_for_resource(resource_id: str):
    # get list of fields from teh resource
    fields = ckan.action.datastore_search(id=resource_id, limit=0)["fields"]

    # {'id': 'PARID', 'info': {'label': 'Parcel Identification Number', 'notes': 'A 16 character unique identifier for the parcel.', 'type_override': ''}, 'type': 'text'}

    # make question for each field
    for field in fields:
        Question(
            id=f"{slugify(field['id'])}",
            name=field.get("info", {}).get("label") or field["id"],
            datatype=field["type"],
        )


# todo: mark resources with metadata for this project
#   - map of regions it describes to the field with the regions' IDs (e.g. {'parcel': 'PARID'}
#   -
