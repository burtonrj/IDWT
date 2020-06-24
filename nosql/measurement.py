import mongoengine


class Measurement(mongoengine.DynamicDocument):
    """
    Base class for measurements.
    """
    patientId = mongoengine.StringField(required=True)
    name = mongoengine.StringField(required=True)
    date = mongoengine.DateField(required=False)
    time = mongoengine.FloatField(required=False)
    requestSource = mongoengine.StringField(required=False)
    notes = mongoengine.StringField(required=False)
    flags = mongoengine.ListField(required=False)

    meta = {
        "db_alias": "core",
        "collection": "testResults",
        "allow_inheritance": True
    }


class ContinuousMeasurement(Measurement):
    result = mongoengine.FloatField(required=True)
    refRange = mongoengine.ListField(required=False)


class DiscreteMeasurement(Measurement):
    result = mongoengine.StringField(required=True)


class ComplexMeasurement(Measurement):
    result = mongoengine.ListField(required=True)
