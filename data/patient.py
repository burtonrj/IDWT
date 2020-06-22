import mongoengine
from data.outcome import Outcome
from data.test_result import TestResult
from data.critical_care import CriticalCare
from Levenshtein import distance as levenshtein_distance


class Comorbidity(mongoengine.DynamicDocument):
    comorbidName = mongoengine.StringField(required=True)

    meta = {
        "db_alias": "core",
        "collection": "comorbid"
    }

    def similarity(self, x: str, edit_threshold: int = 1) -> int:
        if x == self.comorbidName:
            return 2
        if levenshtein_distance(x, self.comorbidName) <= edit_threshold:
            return 1
        return 0


class Patient(mongoengine.Document):
    """
    Document object for a unique individual.

    Parameters
    -----------
    patient_id: str, required
        Unique identifier for patient
    age: int, optional
        Age of the patient
    gender: str, default = "U"
        Patient gender, valid options are: "M", "F", or "U" (male, female or unknown)
    covid: str, default = 0
        COVID-19 status, "Y" = confirmed positive during admission, "N" = negative during entire
        admission, "U" = unknown (if suspected but not confirmed, value will be unknown)
    died: int, default = 0
        1 = patient died during admission, else 0
    critical_care_stay: int, default = 0
        1 = patient had a stay in ICU during admission, else 0
    """

    patient_id = mongoengine.StringField(required=True, unique=True)
    age = mongoengine.IntField(required=False)
    gender = mongoengine.StringField(required=True, default="U", choices=("M", "F", "U"))
    covid = mongoengine.StringField(required=True, default="U", choices=("Y", "N", "U"))
    died = mongoengine.IntField(required=True, default=0, choices=(1, 0))
    criticalCareStay = mongoengine.IntField(required=True, default=0, choices=(1, 0))
    outcomeEvents = mongoengine.ReferenceField(Outcome, reverse_delete_rule=4)
    testResults = mongoengine.ReferenceField(TestResult, reverse_delete_rule=4)
    criticalCare = mongoengine.ReferenceField(CriticalCare, reverse_delete_rule=4)
    comorbidities = mongoengine.ReferenceField(Comorbidity, reverse_delete_rule=4)

    meta = {
        "db_alias": "core",
        "collection": "patients"
    }

    def add_new_outcome(self,
                        event_type: str,
                        component: str or None = None):
        new_outcome = Outcome(event_type=event_type.strip(),
                              )
        if component:
            new_outcome.component = component.strip()



