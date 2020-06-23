import mongoengine


class Outcome(mongoengine.DynamicDocument):
    """
    Document object for a outcome event. All records will relate to a unique Patient via "parent-referencing".
    Outcome events log when a change has happened during a patients stay and are "time-series" in nature.

    Parameters
    -----------
    patient_id: str, required
        Unique patient ID
    component: str, optional
        Even component (for further stratification of event types)
    eventType: str, required
        The type of event
    eventDate: Date, required
        Date of event
    eventTime: float, optional
        Time passed in minutes (relative to 0:00am event date)
    covidStatus: str, (default="U")
        COVID-19 status at time of event (options: "P", "N", "U")
    death: int, (default=0)
        1 = event corresponds to patient death, else 0
    criticalCareAdmission, int (default=0)
        1 = event corresponds to a critical care admission, else 0
    source: str, optional
        Source of event
    sourceType: str, optional
        Type of source
    destination: str, optional
        The patients destination as a result of the event
    wimd: int, optional
        Welsh deprivation score
    """

    patientId = mongoengine.StringField(required=True)
    component = mongoengine.StringField(required=False)
    eventType = mongoengine.StringField(required=True)
    eventDate = mongoengine.DateField(required=True)
    eventTime = mongoengine.FloatField(required=False)
    covidStatus = mongoengine.StringField(default="U", choices=("P", "N", "U"))
    death = mongoengine.IntField(default=0, choices=(1, 0))
    criticalCareAdmission = mongoengine.IntField(default=0, choices=(1, 0))
    source = mongoengine.StringField(required=False)
    sourceType = mongoengine.StringField(required=False)
    destination = mongoengine.StringField(required=False)
    wimd = mongoengine.IntField(required=False)

    meta = {
        "db_alias": "core",
        "collection": "outcomes"
    }
