import mongoengine
import warnings
from .outcome import Outcome
from .measurement import Measurement, ComplexMeasurement, ContinuousMeasurement, DiscreteMeasurement
from .critical_care import CriticalCare
from utilities import parse_datetime
from Levenshtein import distance as levenshtein_distance


def _add_if_value(document, input_variables):
    for name, value in input_variables:
        if value is not None:
            document[name] = value
    return document


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
    outcomeEvents: List(ReferenceField)
        Reference to outcome events, reverse delete rule = Pull (if an outcome event is deleted, it will
        automatically be pulled from this list of references)
    measurements: List(ReferenceField)
        Reference to test results, reverse delete rule = Pull (if a test result is deleted, it will
        automatically be pulled from this list of references)
    criticalCare: List(ReferenceField)
        Reference to critical care record, reverse delete rule = Pull (if a critical care record is deleted, it will
        automatically be pulled from this list of references)
    comorbidities: List(ReferenceField)
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
    measurements = mongoengine.ListField(mongoengine.ReferenceField(Measurement, reverse_delete_rule=4))
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
                           self.measurements,
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
                        wimd: int or None = None,
                        **kwargs):
        """
        Add a new outcome event for patient. Outcome is dynamic, additional parameters can be passed in kwargs.
        Dynamic parameters are not parsed and data type is inferred, to implement explicit support for a new
        parameter contact burtonrj@cardiff.ac.uk

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
        # Parse datetime and check validity (None for date if invalid)
        event_datetime = parse_datetime(event_datetime)
        if event_datetime.get("date") is None:
            raise ValueError(f"Datetime parsed when trying to generate a new outcome event for {self.patient_id}"
                             f"was invalid!")
        # Create outcome document
        new_outcome = Outcome(patientId=self.patient_id,
                              eventType=event_type.strip(),
                              eventDate=event_datetime.get("date"),
                              covidStatus=covid_status,
                              death=death,
                              criticalCareAdmission=critical_care_admission,
                              **kwargs)
        # Populate with optional parameters if given
        new_outcome = _add_if_value(new_outcome, [("component", component),
                                                  ("source", source),
                                                  ("sourceType", source_type),
                                                  ("wimd", wimd),
                                                  ("eventTime", event_datetime.get("time"))])
        new_outcome = new_outcome.save()
        self.outcomeEvents.append(new_outcome)
        self.save()

    def add_new_measurement(self,
                            result,
                            result_type: str,
                            name: str,
                            result_datetime: str or None = None,
                            request_source: str or None = None,
                            result_split_char: str = " ",
                            notes: str or None = None,
                            flags: list or None = None,
                            ref_range: list or None = None,
                            **kwargs):
        if result_datetime is not None:
            result_datetime = result_datetime(result_datetime)
            if result_datetime.get("date") is None:
                raise ValueError(f"Datetime parsed when trying to generate a new measurement document for "
                                 f"{self.patient_id} was invalid!")
        if ref_range:
            assert len(ref_range) == 2, "ref_range should be a list of length two, the first value is the lower " \
                                        "threshold and the second the upper"
        if result_type == "continuous":
            new_result = ContinuousMeasurement(patientId=self.patient_id,
                                               name=name,
                                               result=float(result),
                                               **kwargs)
            new_result = _add_if_value(new_result, [("date", result_datetime.get("date")),
                                                    ("time", result_datetime.get("time")),
                                                    ("requestSource", request_source),
                                                    ("notes", notes),
                                                    ("flags", flags),
                                                    ("ref_range", ref_range)])
        elif result_type == "discrete":
            new_result = DiscreteMeasurement(patientId=self.patient_id,
                                             name=name,
                                             result=str(result),
                                             **kwargs)
            new_result = _add_if_value(new_result, [("date", result_datetime.get("date")),
                                                    ("time", result_datetime.get("time")),
                                                    ("requestSource", request_source),
                                                    ("notes", notes),
                                                    ("flags", flags)])
        elif result_type == "complex":
            result = str(result).split(sep=result_split_char)
            new_result = ComplexMeasurement(patientId=self.patient_id,
                                            name=name,
                                            result=result,
                                            **kwargs)
            new_result = _add_if_value(new_result, [("date", result_datetime.get("date")),
                                                    ("time", result_datetime.get("time")),
                                                    ("requestSource", request_source),
                                                    ("notes", notes),
                                                    ("flags", flags)])
        else:
            raise ValueError("result_type must be one of: 'complex', 'continuous, or 'discrete'")

        new_result = new_result.save()
        self.measurements.append(new_result)
        self.save()

    def get_result_by_type(self, requested_type: str):
        if requested_type == "continuous":
            return [x for x in self.measurements if type(x) == ContinuousMeasurement]
        if requested_type == "discrete":
            return [x for x in self.measurements if type(x) == DiscreteMeasurement]
        if requested_type == "complex":
            return [x for x in self.measurements if type(x) == ComplexMeasurement]
        raise ValueError("request_type must be one of: 'continuous', 'discrete', or 'complex'")


