import mongoengine
from config import GlobalConfig
from .event import Event
from .measurement import Measurement, ComplexMeasurement, ContinuousMeasurement, DiscreteMeasurement
from .critical_care import CriticalCare
from utilities import parse_datetime
from Levenshtein import distance as levenshtein_distance


def _add_if_value(document,
                  input_variables):
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
            1 = strings are similar (edit_distance(x) <= edit threshold)
            0 = strings do not match
        """
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
    def __init__(self, config: GlobalConfig, *args, **values):
        self._config = config
        super(Patient, self).__init__(*args, **values)

    patientId = mongoengine.StringField(required=True, unique=True, primary_key=True)
    age = mongoengine.IntField(required=False)
    gender = mongoengine.StringField(default="U", choices=("M", "F", "U"))
    covid = mongoengine.StringField(default="U", choices=("P", "N", "U"))
    died = mongoengine.IntField(default=0, choices=(1, 0))
    criticalCareStay = mongoengine.IntField(default=0, choices=(1, 0))
    outcomeEvents = mongoengine.ListField(mongoengine.ReferenceField(Event, reverse_delete_rule=4))
    measurements = mongoengine.ListField(mongoengine.ReferenceField(Measurement, reverse_delete_rule=4))
    criticalCare = mongoengine.ListField(mongoengine.ReferenceField(CriticalCare, reverse_delete_rule=4))
    comorbidities = mongoengine.ListField(mongoengine.ReferenceField(Comorbidity, reverse_delete_rule=4))

    meta = {
        "db_alias": "core",
        "collection": "patients"
    }

    def delete(self,
               signal_kwargs=None,
               **write_concern):
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
        self._config.write_to_log(f"Deleting {self.patientId} and associated documents...")
        for references in [self.outcomeEvents,
                           self.measurements,
                           self.criticalCare]:
            for doc in references:
                doc.delete(signal_kwargs=signal_kwargs, **write_concern)
        super().delete(self=self,
                       signal_kwargs=signal_kwargs,
                       **write_concern)
        self._config.write_to_log(f"Deleted patient and asssociated documents.")

    def add_new_event(self,
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
            err = f"Datetime parsed when trying to generate a new outcome event for {self.patientId} was invalid!"
            self._config.write_to_log(err)
            raise ValueError(err)
        # Create outcome document
        new_outcome = Event(patientId=self.patientId,
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
        self._config.write_to_log(f"Outcome event {new_outcome.id} for patient {self.patientId}")

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
        """
        Add a new measurement for a patient. The user should specify a measurement type using the argument 'result_type'

        Parameters
        ----------
        result
        result_type
        name
        result_datetime
        request_source
        result_split_char
        notes
        flags
        ref_range
        kwargs

        Returns
        -------

        """
        if result_datetime is not None:
            result_datetime = result_datetime(result_datetime)
            if result_datetime.get("date") is None:
                err = f"Datetime parsed when trying to generate a new measurement document for " \
                      f"{self.patientId} was invalid!"
                self._config.write_to_log(err)
                raise ValueError(err)
        if ref_range:
            assert len(ref_range) == 2, "ref_range should be a list of length two, the first value is the lower " \
                                        "threshold and the second the upper"
        if result_type == "continuous":
            new_result = ContinuousMeasurement(patientId=self.patientId,
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
            new_result = DiscreteMeasurement(patientId=self.patientId,
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
            new_result = ComplexMeasurement(patientId=self.patientId,
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
        self._config.write_to_log(f"Measurement {new_result.id} added for patient {self.patientId}")

    def add_new_comorbidity(self,
                            name: str,
                            conflicts: str = "ignore",
                            edit_threshold: int = 1,
                            **kwargs):
        existing = Comorbidity.objects(comorbidName=name)
        if existing:
            existing = existing.get()
            self.comorbidities.append(existing)
            self.save()
            self._config.write_to_log(f"Associated patient {self.patientId} too {existing.comorbidName}")
            return None
        similar = [comorb for comorb in Comorbidity.objects()
                   if comorb.similarity(x=name, edit_threshold=edit_threshold) == 1]
        if conflicts == "ignore" or len(similar) == 0:
            new_comorb = Comorbidity(cmorbidName=name, **kwargs)
            self.comorbidities.append(new_comorb)
            self.save()
            self._config.write_to_log(f"Associated patient {self.patientId} too {name}")
            return None
        else:
            if len(similar) > 1 or conflicts == "raise":
                err = f"Multiple similar comorbitities found when entering {name} for patient {self.patientId}"
                self._config.write_to_log(err)
                raise ValueError(err)
            if conflicts == "merge":
                self.comorbidities.append(similar[0])
                self.save()
                self._config.write_to_log(f"Associated patient {self.patientId} too {similar[0].comorbidName}")
        raise ValueError("conflicts argument should be one of: 'ignore', 'raise', or 'merge'")

    def add_new_critical_care(self,
                              admission_datetime: str or None = None,
                              discharge_datetime: str or None = None,
                              request_location: str or None = None,
                              icu_days: float or None = None,
                              ventilated: str = "U",
                              covid_status: str = "U",
                              **kwargs):

        admission_datetime = parse_datetime(admission_datetime)
        discharge_datetime = parse_datetime(discharge_datetime)
        new_event = CriticalCare(patientId=self.patientId, **kwargs)
        new_event = _add_if_value(new_event, [("admissionDate", admission_datetime.get("date")),
                                              ("admissionTime", admission_datetime.get("time")),
                                              ("dischargeDate", discharge_datetime.get("date")),
                                              ("dischargeTime", discharge_datetime.get("time")),
                                              ("requestLocation", request_location),
                                              ("icuDays", icu_days),
                                              ("ventilated", ventilated),
                                              ("covidStatis", covid_status)])
        self.criticalCare.append(new_event)
        self.save()
        self._config.write_to_log(f"New critical care event added for patient {self.patientId}")

    def get_measurement_by_type(self, requested_type: str):
        """
        Filter measurements by data type, either continuous, discrete or complex

        Parameters
        ----------
        requested_type: str
            'continuous', 'discrete' or 'complex'

        Returns
        -------
        list
            List of filtered Measurement objects
        """
        if requested_type == "continuous":
            return [x for x in self.measurements if type(x) == ContinuousMeasurement]
        if requested_type == "discrete":
            return [x for x in self.measurements if type(x) == DiscreteMeasurement]
        if requested_type == "complex":
            return [x for x in self.measurements if type(x) == ComplexMeasurement]
        raise ValueError("request_type must be one of: 'continuous', 'discrete', or 'complex'")



