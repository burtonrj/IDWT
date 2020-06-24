from .patient import Patient
from config import GlobalConfig
from collections import defaultdict
import pandas as pd
import os
import re


class PopulateUpdate:
    def __init__(self,
                 config: GlobalConfig,
                 target_directory: str,
                 id_column: str,
                 conflicts: str = "raise"):
        assert os.path.isdir(target_directory), f"Target directory {target_directory} does not exist!"
        self._config = config
        self._files = self._parse_files(target_directory)
        self._id_column = self._check_id_column(id_column=id_column)
        self._patients = self._patient_indexes()
        self.conflicts = conflicts

    @property
    def conflicts(self):
        return self._conflicts

    @conflicts.setter
    def conflicts(self, value: str):
        assert value in ["raise", "ignore"], "Invalid value, must be 'raise' or 'ignore'"
        self._conflicts = value

    @staticmethod
    def _load_dataframe(path: str, filetype: str, index: pd.Index or None = None, **kwargs):
        if filetype == "csv":
            df = pd.read_csv(path, **kwargs)
        elif filetype == "excel":
            df = pd.read_excel(path, **kwargs)
        else:
            raise ValueError("filetype must be 'csv' or 'excel'")
        if index is not None:
            return df.loc[index]
        return df

    @staticmethod
    def _parse_files(target_directory: str):
        files = dict()
        for filename in os.listdir(target_directory):
            if filename.endswith(".csv"):
                files[filename] = {"path": os.path.join(target_directory, filename),
                                   "type": "csv"}
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                files[filename] = {"path": os.path.join(target_directory, filename),
                                   "type": "excel"}
        return files

    def _check_id_column(self, id_column: str):
        for name, properties in self._files.items():
            temp_df = self._load_dataframe(properties.get("path"),
                                           filetype=properties.get("type"),
                                           nrows=3)
            assert id_column in temp_df.columns, f"{name} does not contain primary id column {id_column}"
        return id_column

    @staticmethod
    def _filter_columns(columns: iter, regex_terms: iter):
        return list(filter(lambda x: any([re.match(pattern=p, string=x, flags=re.IGNORECASE) for p in regex_terms]),
                           columns))

    def _patient_indexes(self):
        # Search through every file and generate a dictionary of unique patient indexes
        patients = defaultdict(dict)
        for name, properties in self._files.items():
            pt_ids = self._load_dataframe(path=properties.get("path"),
                                          filetype=properties.get("type"),
                                          usecols=[self._id_column])[self._id_column]
            for _id in pt_ids.unique():
                patients[str(_id)][name] = pt_ids[pt_ids == _id].index
        return patients

    def _load_pt_dataframe(self, patient_id: str):
        patient_idx = self._patients.get(patient_id)
        for name, properties in self._files.items():
            df = self._load_dataframe(path=properties.get("path"),
                                      filetype=properties.get("type"),
                                      index=patient_idx.get(name))
            if df.shape[0] == 0:
                continue
            yield name, df

    def _pt_search_multi(self,
                         patient_id: str,
                         column_search_terms: list):
        all_values = dict()
        for filename, df in self._load_pt_dataframe(patient_id=patient_id):
            columns = self._filter_columns(columns=df.columns,
                                           regex_terms=column_search_terms)
            file_values = df[columns].values.flatten()
            if len(file_values) == 0:
                continue
            all_values[filename] = list(set(file_values))
        unique_values = list(set([v for nested in all_values.values() for v in nested]))
        if len(unique_values) == 0:
            return None
        return unique_values

    def _pt_search(self,
                   patient_id: str,
                   column_search_terms: list,
                   variable_name: str):
        all_values = dict()
        for filename, df in self._load_pt_dataframe(patient_id=patient_id):
            columns = self._filter_columns(columns=df.columns,
                                           regex_terms=column_search_terms)
            file_values = df[columns].values.flatten()
            if len(file_values) == 0:
                continue
            if len(set(file_values)) > 1:
                err = f"Conflicting {variable_name} values found for patient {patient_id} in file {filename}, " \
                      f"if conflict option set to ignore, this file will be ignored from this process"
                self._config.write_to_log(err)
                if self.conflicts == "raise":
                    raise ValueError(err)
            else:
                all_values[filename] = file_values[0]
        n_unique = len(set(all_values.values()))
        if n_unique > 1:
            err = f"Conflicting {variable_name} values found for patient {patient_id}. " \
                  f"{variable_name} registered in each file: {all_values}"
            self._config.write_to_log(err)
            if self.conflicts == "raise":
                raise ValueError(err)
            return None
        elif n_unique == 0:
            return None
        return all_values[list(all_values.keys())[0]]

    def _patient_death(self,
                       patient_id: str,
                       death_options: dict):
        death_file = death_options.get("death_file")
        death_column = death_options.get("death_column")
        search_terms = death_options.get("search_terms")
        files = [(filename, df) for filename, df in self._load_pt_dataframe(patient_id=patient_id)
                 if death_file.lower() in filename.lower()]
        if len(files) == 0:
            return 0
        for filename, df in files:
            if any(re.match(pattern=st, string=df[death_column].values, flags=re.IGNORECASE) for st in search_terms):
                return 1
        return 0

    def _patient_critical_care_stay(self,
                                    patient_id: str,
                                    critical_care_options: dict):
        critical_care_file = critical_care_options.get("critical_care_file")
        presence_infers_positivity = critical_care_options.get("presence_infers_positivity")
        critical_care_column = critical_care_options.get("critical_care_column")
        critical_care_pos_value = critical_care_options.get("critical_care_pos_value")

        files = [(filename, df) for filename, df in self._load_pt_dataframe(patient_id=patient_id)
                 if critical_care_file.lower() in filename.lower()]
        if presence_infers_positivity and len(files) > 0:
            return 1
        elif critical_care_pos_value is None or critical_care_column is None:
            raise ValueError("If presence_infers_positivity is False, pos_value and column name must be given")
        else:
            files = list(filter(lambda x: critical_care_pos_value in x[1][critical_care_column], files))
            if len(files) >= 1:
                return 1
        return 0

    def _fetch_patient_basics(self,
                              patient_id: str,
                              search_terms: dict,
                              death_options:dict,
                              gender_int_mappings: dict,
                              critical_care_options: dict):
        # Fetch age
        age = self._pt_search(patient_id=patient_id,
                              column_search_terms=search_terms.get("age_search_terms"),
                              variable_name="age")

        # Determine if patient has ever stayed in critical care during admission
        critical_care_stay = self._patient_critical_care_stay(patient_id=patient_id,
                                                              critical_care_options=critical_care_options)

        # Fetch gender
        gender = self._pt_search(patient_id=patient_id,
                                 column_search_terms=search_terms.get("gender_search_terms"),
                                 variable_name="gender")
        # Process gender
        if gender is not None:
            if type(gender) == float:
                gender = int(gender)
            if type(gender) == int:
                if gender not in gender_int_mappings.keys():
                    raise ValueError(f"Gender returned value {gender} for patient {patient_id}, but value not present "
                                     f"in given mappings")
                gender = gender_int_mappings[gender]
            elif re.match(pattern="m[ale]*", string=gender, flags=re.IGNORECASE):
                gender = "M"
            elif re.match(pattern="f[emale]*", string=gender, flags=re.IGNORECASE):
                gender = "F"
            else:
                gender = "U"

        # Fetch covid status
        covid = self._pt_search_multi(patient_id=patient_id,
                                      column_search_terms=search_terms.get("covid_search_terms"))
        # Filter covid status and summarise
        cst = search_terms.get("covid_status_search_terms")
        filtered_values = {status: list(filter(lambda x: any(re.match(pattern=p, string=x, flags=re.IGNORECASE)
                                                             for p in patterns), covid))
                           for status, patterns in cst.keys()}
        if len(filtered_values["positive"]) == 0 and len(filtered_values["negative"]) >= 1:
            covid_status = "N"
        elif len(filtered_values["positive"]) > 0:
            covid_status = "P"
        else:
            covid_status = "U"

        # Fetch events of death
        died = self._patient_death(patient_id=patient_id,
                                   death_options=death_options)
        return dict(age=age, gender=gender, covid=covid_status, died=died, critical_care_stay=critical_care_stay)

    def _assert_patients_added(self,
                               patient_ids: list):
        if self._config.db_type == "nosql":
            for pt_id in patient_ids:
                assert len(Patient.objects(patientId=str(pt_id))) > 0, \
                    f"{pt_id} missing from database, have you called add_patients?"
        else:
            curr = self._config.db_connection.cursor()
            for pt_id in patient_ids:
                curr.execute("""SELECT patient_id FROM patients WHERE patient_id=?""", pt_id)
                assert len(curr.fetchall()) > 0, f"{pt_id} missing from database, have you called add_patients?"

    def add_patients(self,
                     conflicts: str = "raise",
                     age_search_terms: list or None = None,
                     gender_search_terms: list or None = None,
                     covid_search_terms: list or None = None,
                     covid_status_search_terms: list or None = None,
                     death_search_terms: list or None = None,
                     death_column: str = "destination",
                     death_file: str = "outcome",
                     gender_int_mappings: dict or None = None,
                     critical_care_file: str = "outcome",
                     critical_care_presence_infers_positivity: bool = False,
                     critical_care_column: str = "CRITICAL_CARE",
                     critical_care_pos_value: str = "Y"):
        if age_search_terms is None:
            age_search_terms = ["^age$", "^age[.-_]+", "[.-_]+age"]
        if gender_search_terms is None:
            gender_search_terms = ["^gender$", "^gender[.-_]+", "[.-_]+gender", "^sex$", "^sex[.-_]+", "[.-_]+sex"]
        if covid_search_terms is None:
            covid_search_terms = ["covid_status", "covid", "covid19"]
        if covid_status_search_terms is None:
            covid_status_search_terms = {"positive": ["+ve", "^p$", "^pos$", "^positive$"],
                                         "negative": ["-ve", "^p$", "^neg$", "^negative$"],
                                         "suspected": ["suspected"]}
        if gender_int_mappings is None:
            gender_int_mappings = {0: "M",
                                   1: "F"}
        if death_search_terms is None:
            death_search_terms = ["dead", "died", "death"]
        search_terms = dict(age_search_terms=age_search_terms,
                            gender_search_terms=gender_search_terms,
                            covid_search_terms=covid_search_terms,
                            death_search_terms=death_search_terms,
                            covid_status_search_terms=covid_status_search_terms)
        death_options = dict(death_column=death_column,
                             death_file=death_file)
        critical_care_options = dict(critical_care_file=critical_care_file,
                                     critical_care_presence_infers_positivity=critical_care_presence_infers_positivity,
                                     critical_care_column=critical_care_column,
                                     critical_care_pos_value=critical_care_pos_value)

        self.conflicts = conflicts
        # Search the unique patients and check if they already exist, if they don't add them
        if self._config.db_type == "nosql":
            for pt_id in self._patients.keys():
                basics = self._fetch_patient_basics(patient_id=pt_id,
                                                    search_terms=search_terms,
                                                    death_options=death_options,
                                                    critical_care_options=critical_care_options,
                                                    gender_int_mappings=gender_int_mappings)
                patient = Patient(patientId=pt_id,
                                  config=self._config)
                for key, value in basics.items():
                    if value is None:
                        continue
                    patient[key] = value
                patient.save()
                self._config.write_to_log(f"New patient {pt_id} written to Patients collection")
        else:
            for pt_id in self._patients.keys():
                basics = self._fetch_patient_basics(patient_id=pt_id,
                                                    search_terms=search_terms,
                                                    death_options=death_options,
                                                    critical_care_options=critical_care_options,
                                                    gender_int_mappings=gender_int_mappings)
                patient = {"patient_id": [pt_id]}
                for key, value in basics.items():
                    if value is not None:
                        patient[key] = [value]
                patient = pd.DataFrame(patient)
                patient.to_sql(name="patients",
                               con=self._config.db_connection,
                               if_exists="append")
                self._config.db_connection.commit()
                self._config.write_to_log(f"New patient {pt_id} written to patients table")

    def add_outcome_events(self,
                           filename: str,
                           mappings: dict):
        outcome_files = {name: properties for name, properties in self._files.items()
                         if filename.lower() in name.lower()}
        outcomes = pd.concat([self._load_dataframe(path=properties.get("path"),
                                                   filetype=properties.get("type"))
                              for properties in outcome_files.values()], ignore_index=True)
        patient_ids = outcomes[self._id_column].values
        self._assert_patients_added(patient_ids=patient_ids)
        if self._config.db_type == "nosql":
            try:
                for pt_id in patient_ids:
                    patient = Patient.objects(patientId=str(pt_id)).get()
                    patient.add_new_outcome(event_type=mappings["event_type"],
                                            event_datetime=mappings["event_datetime"],
                                            covid_status=mappings["covid_status"],
                                            death=mappings["death"],
                                            critical_care_admission=mappings["critical_care_admission"],
                                            component=mappings.get("component"),
                                            source_type=mappings.get("source_type"),
                                            source=mappings.get("source"),
                                            wimd=mappings.get("wimd"))
            except KeyError:
                raise KeyError("Mappings invalid, must contain keys: event_type, event_datetime, covid_status, "
                               "death, critical_care_admission")
        else:
            assert "patient_id" in mappings.keys(), "patient_id missing from mapping keys"
            mappings = {value: key for key, value in mappings.items()}
            outcomes.rename(columns=mappings)
            outcomes.to_sql(name="Outcome",
                            con=self._config.db_connection,
                            if_exists="fail")

    def add_measurements(self):
        pass

    def add_critical_care_events(self):
        pass

    def add_comorbidities(self):
        pass

