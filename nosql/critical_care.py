import mongoengine


class CriticalCare(mongoengine.DynamicDocument):
    patientId = mongoengine.StringField(required=True)
    admissionDate = mongoengine.DateField(required=False)
    admissionTime = mongoengine.FloatField(required=False)
    dischargeDate = mongoengine.DateField(required=False)
    dischargeTime = mongoengine.FloatField(required=False)
    requestLocation = mongoengine.StringField(required=False)
    icuDays = mongoengine.FloatField(required=False)
    ventilated = mongoengine.StringField(default="U", choices=("Y", "N", "U"))
    covidStatus = mongoengine.StringField(default="U", choices=("Y", "N", "U"))

    meta = {
        "db_alias": "core",
        "collection": "criticalCare"
    }

