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
        return list(filter(lambda x: any([re.match(pattern=p, string=x) for p in regex_terms]),
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

    def _pt_search(self,
                   patient_id: str,
                   column_search_terms: list,
                   variable_name: str,
                   multi_value_output: bool = False):
        all_values = dict()
        for filename, df in self._load_pt_dataframe(patient_id=patient_id):
            columns = self._filter_columns(columns=df.columns,
                                           regex_terms=column_search_terms)
            file_values = df[columns].values.flatten()
            if multi_value_output:
                all_values[filename] = set(file_values)
                continue
            if len(set(file_values)) > 1:
                err = f"Conflicting {variable_name} values found for patient {patient_id} in file {filename}, " \
                      f"if conflict option set to ignore, this file will be ignored from this process"
                self._config.write_to_log(err)
                if self.conflicts == "raise":
                    raise ValueError(err)
            else:
                all_values[filename] = file_values[0]
        if multi_value_output:
            return all_values
        if len(set(all_values.values())) > 1:
            err = f"Conflicting {variable_name} values found for patient {patient_id}. " \
                  f"{variable_name} registered in each file: {all_values}"
            self._config.write_to_log(err)
            if self.conflicts == "raise":
                raise ValueError(err)
            return None
        return all_values[list(all_values.keys())[0]]

    def _patient_death(self,
                       patient_id: str,
                       death_column: str,
                       search_terms: list,
                       death_file: str):
        files = [(filename, df) for filename, df in self._load_pt_dataframe(patient_id=patient_id)
                 if death_file in filename.lower()]
        if len(files) == 0:
            return 0
        for filename, df in files:
            if any(re.match(pattern=st, string=df[death_column].values) for st in search_terms):
                return 1

    def _fetch_patient_basics(self,
                              patient_id: str,
                              search_terms: dict,
                              death_column: str,
                              death_file: str):
        age = self._pt_search(patient_id=patient_id,
                              column_search_terms=search_terms.get("age_search_terms"),
                              variable_name="age")
        gender = self._pt_search(patient_id=patient_id,
                                 column_search_terms=search_terms.get("gender_search_terms"),
                                 variable_name="gender")
        covid = self._pt_search(patient_id=patient_id,
                                column_search_terms=search_terms.get("covid_search_terms"),
                                variable_name="covid_status",
                                multi_value_output=True)
        # TODO: search covid results and determine if positive status at any point
        died = self._patient_death(patient_id=patient_id,
                                   death_column=death_column,
                                   search_terms=search_terms.get("death_search_terms"),
                                   death_file=death_file)
        return dict(age=age, gender=gender, covid=covid, died=died)

    def add_new_patients(self,
                         conflicts: str = "raise",
                         age_search_terms: list or None = None,
                         gender_search_terms: list or None = None,
                         covid_search_terms: list or None = None,
                         covid_pos_search_terms: list or None = None,
                         death_search_terms: list or None = None,
                         death_column: str = "destination",
                         death_file: str = "outcome"):
        if age_search_terms is None:
            age_search_terms = ["^age$", "^age[.-_]+", "[.-_]+age"]
        if gender_search_terms is None:
            gender_search_terms = ["^gender$", "^gender[.-_]+", "[.-_]+gender", "^sex$", "^sex[.-_]+", "[.-_]+sex"]
        if covid_search_terms is None:
            covid_search_terms = ["covid_status", "covid", "covid19"]
        if covid_pos_search_terms is None:
            covid_pos_search_terms = ["+ve", "^p$", "^pos$", "^positive$"]
        if death_search_terms is None:
            death_search_terms = ["dead", "died", "death"]
        search_terms = dict(age_search_terms=age_search_terms,
                            gender_search_terms=gender_search_terms,
                            covid_search_terms=covid_search_terms,
                            death_search_terms=death_search_terms)

        self.conflicts = conflicts
        # Search the unique patients and check if they already exist, if they don't add them
        if self._config.db_type == "nosql":
            new_patients = {_id: indexes for _id, indexes in self._patients.items()
                            if not Patient.objects(patientId=_id)}
            for pt_id in new_patients.keys():
                basics = self._fetch_patient_basics(patient_id=pt_id,
                                                    search_terms=search_terms,
                                                    death_column=death_column,
                                                    death_file=death_file)



    def add_outcome_events(self):
        pass

    def add_measurements(self):
        pass

    def add_critical_care_events(self):
        pass

    def add_comorbidities(self):
        pass

