import mongoengine
import warnings
from .outcome import Outcome
from .test_result import TestResult
from .critical_care import CriticalCare
from utilities import parse_datetime
from Levenshtein import distance as levenshtein_distance


class Comorbidity(mongoengine.DynamicDocument):
    """
    Collection contains all possible comorbidities. If a patient is associated to a comorbidity documented
    then it is assumed that the patient has said comorbidity

    Parameters
    ----------
    comorbidName: str, required
        Name of the comorbidity

    """
    comorbidName = mongoengine.StringField(required=True)

    meta = {
        "db_alias": "core",
        "collection": "comorbid"
    }

    def similarity(self,
                   x: str,
                   edit_threshold: int = 1) -> int:
        """
        Using edit distance (levenshtein distance), assess the similarity of a given string to the comorbidity name

        Parameters
        ----------
        x: str
            String for comparison
        edit_threshold: int (default=1)
            Threshold for edit distance, if less than or equal to threshold, then return positive similarity score
        Returns
        -------
        int
            2 = strings are a perfect match
            1 = strings are similar (edit_distance(x) <= edit threshold)
            0 = strings do not match
        """
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
    outcomeEvents: ReferenceField
        Reference to outcome events, reverse delete rule = Pull (if an outcome event is deleted, it will
        automatically be pulled from this list of references)
    testResults: ReferenceField
        Reference to test results, reverse delete rule = Pull (if a test result is deleted, it will
        automatically be pulled from this list of references)
    criticalCare: ReferenceField
        Reference to critical care record, reverse delete rule = Pull (if a critical care record is deleted, it will
        automatically be pulled from this list of references)
    comorbidities: ReferenceField
        Reference to associated comorbidities, reverse delete rule = Pull (if a type of comorbidity is deleted, it will
        automatically be pulled from this list of references)
    """

    patient_id = mongoengine.StringField(required=True, unique=True)
    age = mongoengine.IntField(required=False)
    gender = mongoengine.StringField(required=True, default="U", choices=("M", "F", "U"))
    covid = mongoengine.StringField(required=True, default="U", choices=("Y", "N", "U"))
    died = mongoengine.IntField(required=True, default=0, choices=(1, 0))
    criticalCareStay = mongoengine.IntField(required=True, default=0, choices=(1, 0))
    outcomeEvents = mongoengine.ListField(mongoengine.ReferenceField(Outcome, reverse_delete_rule=4))
    testResults = mongoengine.ListField(mongoengine.ReferenceField(TestResult, reverse_delete_rule=4))
    criticalCare = mongoengine.ListField(mongoengine.ReferenceField(CriticalCare, reverse_delete_rule=4))
    comorbidities = mongoengine.ListField(mongoengine.ReferenceField(Comorbidity, reverse_delete_rule=4))

    meta = {
        "db_alias": "core",
        "collection": "patients"
    }

    def delete(self, signal_kwargs=None, **write_concern):
        """
        Method override for parent delete method. Removes all unique referenced documents for patient prior to delete.

        Parameters
        ----------
        signal_kwargs: dict, optional
            kwargs dictionary to be passed to the signal calls.
        write_concern
             Extra keyword arguments are passed down which will be used as options for the resultant getLastError
             command. For example, save(..., w: 2, fsync: True) will wait until at least two servers have recorded
             the write and will force an fsync on the primary server
        Returns
        -------
        None
        """
        for references in [self.outcomeEvents,
                           self.testResults,
                           self.criticalCare]:
            for doc in references:
                doc.delete(signal_kwargs=signal_kwargs, **write_concern)
        super().delete(self=self,
                       signal_kwargs=signal_kwargs,
                       **write_concern)

    def add_new_outcome(self,
                        event_type: str,
                        event_datetime: str,
                        covid_status: str = "U",
                        death: int = 0,
                        critical_care_admission: int = 0,
                        component: str or None = None,
                        source: str or None = None,
                        source_type: str or None = None,
                        wimd: int or None = None):
        """
        Add a new outcome event for patient.

        Parameters
        ----------
        event_type: str, required
            populates eventType in Outcome
        event_datetime: str, required
            Date or DateTime string. Can handle variable formats but requires that in date, day precedes month, which
            precedes year (see utilities.parse_datetime for more details). Parsed datetime populates eventDate and
            eventTime in Outcome.
        covid_status: str, required, (default="U")
            populates covidStatus in Outcome
        death: int, required, (default=0)
            populates death in Outcome
        critical_care_admission: int, required, (default=0)
            populates criticalCareAdmission in Outcome
        component: str, optional
            populates component in Outcome
        source: str, optional
            populates source in Outcome
        source_type: str, optional
            populates sourceType in Outcome
        wimd: int, optional
            populates wimd in Outcome

        Returns
        -------
        None
        """
        event_datetime = parse_datetime(event_datetime)
        if event_datetime.get("date") is None:
            warnings.warn(f"Datetime parsed when trying to generate a new outcome event for {self.patient_id}"
                          f"was invalid!")
        new_outcome = Outcome(event_type=event_type.strip(),
                              event_date=event_datetime.get("date"),
                              covid_status=covid_status,
                              death=death,
                              critical_care_admission=critical_care_admission)
        for name, value in [("component", component),
                            ("source", source),
                            ("sourceType", source_type),
                            ("wimd", wimd),
                            ("eventTime", event_datetime.get("time"))]:
            if value is not None:
                new_outcome[name] = value
        new_outcome.save()
        self.outcomeEvents.append(new_outcome)
        self.save()

    def add_new

