import mongoengine


class Outcome(mongoengine.Document):
    """
    Document object for a outcome event. All records will relate to a unique Patient via "parent-referencing".
    Outcome events log when a change has happened during a patients stay and are "time-series" in nature.

    Parameters
    -----------
    patient_id: str, required
        Unique patient ID
    """

    patientId = mongoengine.StringField(required=True)
    component = mongoengine.StringField(required=False)
    eventType = mongoengine.StringField(required=True)
    eventDate = mongoengine.DateField(required=True)
    covidStatus = mongoengine.StringField(required=True, default="U", choices=("Y", "N", "U"))
    death = mongoengine.IntField(required=True, default=0, choices=(1, 0))
    criticalCareAdmission = mongoengine.IntField(required=True, default=0, choices=(1, 0))
    source = mongoengine.StringField(required=False)
    sourceType = mongoengine.StringField(required=False)
    destination = mongoengine.StringField(required=True)
    wimd = mongoengine.IntField(required=False)

    meta = {
        "db_alias": "core",
        "collection": "outcomes"
    }
