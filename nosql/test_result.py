import mongoengine


class TestResult(mongoengine.DynamicDocument):
    patientId = mongoengine.StringField(required=True)
    testName = mongoengine.StringField(required=True)
    testResult_str = mongoengine.StringField(required=False)
    testResultNum = mongoengine.FloatField(required=False)
    testDate = mongoengine.DateField(required=False)
    testTime = mongoengine.FloatField(required=False)

    meta = {
        "db_alias": "core",
        "collection": "testResults"
    }