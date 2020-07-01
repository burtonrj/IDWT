from nosql.patient import Patient
from config import GlobalConfig
from collections import defaultdict
import pandas as pd
import os
import re


class Populate:
    """
    Populate database from tabular files

    Parameters
    -----------
    config: GlobalConfig
        Instance of GlobalConfig, defining application wide options
    target_directory: str
        Path to directory containing tabular files
    id_column: str
        Name of column containing unique patient identifier. Must be common to all tabular files, if not AssertionError
        will be raised
    conflicts: str
        How to handle conflicting data entry e.g. if a patient has conflicting age values in the same file or across
        multiple files.
        "raise" - throws ValueError
        "ignore" - ignored and patient is skipped, but event is logged
    """
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
    def _load_dataframe(path: str,
                        filetype: str,
                        index: pd.Index or None = None,
                        **kwargs) -> pd.DataFrame:
        """
        Load a tabular file as a Pandas DataFrame, with options to filter by index

        Parameters
        ----------
        path: str
            Path of file to load
        filetype: str
            Should be either: 'csv' or 'excel'
        index: Pandas.Index (optional)
            If given, returned DataFrame is filtered to return only given index
        kwargs:
            Additional keyword arguments passed to Pandas.DataFrame.read_csv/.read_excel call

        Returns
        -------
        Pandas.DataFrame
        """
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
    def _parse_files(target_directory: str) -> dict:
        """
        Given a target directory, parse files in directory, filter to keep files that are tabular (either csv files or
        excel files) and generate a dictionary object where the key is the file name and the value is a nested
        dictionary containing the file path and the type of file (either "csv" or "excel").
        Example:
            {"file_1.csv": {"path": "C:/path/to/files/file_1.csv", "type": "csv"}}

        Parameters
        ----------
        target_directory: str
            Path to directory containing target files

        Returns
        -------
        dict
        """
        files = dict()
        for filename in os.listdir(target_directory):
            if filename.endswith(".csv"):
                files[filename] = {"path": os.path.join(target_directory, filename),
                                   "type": "csv"}
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                files[filename] = {"path": os.path.join(target_directory, filename),
                                   "type": "excel"}
        return files

    def _check_id_column(self, id_column: str) -> str:
        """
        Check that the designated identifier column is present in all files in the target directory. If a file lacks
        this column, an AssertionError will be raised

        Parameters
        ----------
        id_column: str
            Name of the designated identifier column

        Returns
        -------
        str
            Name of identifier column
        """
        for name, properties in self._files.items():
            temp_df = self._load_dataframe(properties.get("path"),
                                           filetype=properties.get("type"),
                                           nrows=3)
            assert id_column in temp_df.columns, f"{name} does not contain primary id column {id_column}"
        return id_column

    @staticmethod
    def _filter_columns(columns: list,
                        regex_terms: list):
        """
        Given the list of columns in a DataFrame and a list of regular expression patterns to be used as search terms,
        return the filtered list of columns such that only columns that match one or more of the search terms are kept

        Parameters
        ----------
        columns: list
            List of column names
        regex_terms: list
            List of regex search term patterns

        Returns
        -------
        list
            Filtered list of column names
        """
        return list(filter(lambda x: any([re.match(pattern=p, string=x, flags=re.IGNORECASE) for p in regex_terms]),
                           columns))

    def _patient_indexes(self):
        """
        Search through all target files and generate a nested dictionary of patient IDs and the index values
        corresponding to each patient for each file, example:
        {"patient1": {"file1": [2,5,7,33],
                      "file2": [5,8,11,23]}
        Returns
        -------
        dict
            Nested dictionary of unique patients and corresponding file indexes
        """
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
        """
        For a given patient ID, yield the DataFrame for each target file, filtered to contain only rows that
        correspond to the given patient. Only yields DataFrames with 1 or more rows.

        Parameters
        ----------
        patient_id: str

        Returns
        -------
        Pandas.DataFrame
        """
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
                         column_search_terms: list) -> list or None:
        """
        Use when expecting a multi-value output. Iterates over all target files and given a patient ID and a list
        of regex search patterns, loads the DataFrame for the given patient for each file, filters to keep only
        columns matching the search patterns, and returns all unique values for columns across all files.

        Parameters
        ----------
        patient_id: str
            Patient identifier
        column_search_terms: list
            List of regular expressions
        Returns
        -------
        list or None
            If 1 or more values found, return list of unique values, else return None
        """
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
                   variable_name: str) -> str or int or float or None:
        """
        Use when expecting a singular value returned. Iterates over all target files and given a patient ID and a list
        of regex search patterns, loads the DataFrame for the given patient for each file, filters to keep only
        columns matching the search patterns, and returns all unique values for columns across all files.

        Expects that, once removing duplicates, only one unique value will remain. If more than one value is found
        for the chosen columns across all DataFrames, will throw a ValueError if conflicts setting = "raise". If
        conflicts setting = "ignore", will return None.

        Parameters
        ----------
        patient_id: str
            Patient identifier
        column_search_terms: list
             List of regular expressions
        variable_name: str
            Common name of the variable being searched for, used for error logging.

        Returns
        -------
        str or int or float or None
            Returns None if multiple unique values are found and conflicts setting = "ignore", else should return
            single value of variable type
        """
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
                       death_options: dict) -> int:
        """
        Search target files for evidence of death during admission

        Parameters
        ----------
        patient_id: str
            Patient identifier
        death_options: dict
            Dictionary of specific options, keys and values as follows:
                death_file - which target file to search for events of death
                death_column - which column in the target file to search for events of death
                search_terms - list of regular expressions to match, if any match return 1, else return 0
        Returns
        -------
        int
            1 if an event of death is found, else returns 0
        """
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
                                    critical_care_options: dict) -> int:
        """
        Search target files for a critical care stay event

        Parameters
        ----------
        patient_id: str
            Patient identifier
        critical_care_options: dict
            Dictionary of specific options, keys and values as follows:
                critical_care_file - which target file to search for events of critical care admission
                presence_infers_positivity - if True, the presence of the patient in the target file is
                inferred as being positive for "critical care admission"
                critical_care_column - if presence_infers_positivity is False, this is the column in the target file
                that is searched for a positive value as specified by critical_care_pos_value
                critical_care_pos_value - the value to search for in critical_care_column in target file
                to determine positivity if presence_infers_positivity is False

        Returns
        -------
        int
            1 if critical care admission is found, else 0
        """
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
                              death_options: dict,
                              gender_int_mappings: dict,
                              critical_care_options: dict) -> dict:
        """
        Given some patient, determine the most basic information using the collection of target files. This
        includes: age, gender, COVID-19 status, event of death, and event of critical care admission

        Parameters
        ----------
        patient_id: str
            Patient identifier
        search_terms: dict
            Dictionary of regular expression search terms to use for dynamic search of column names (in the case of
            variables where a single unique value is expected for each patient) and positive values (in the case of
            variables where multiple values might exist and you need to further classifier values e.g. positive or
            negative).
            Expects the following key value entries:
                age_search_terms - search terms for matching column name that corresponds to age
                gender_search_terms - search terms for matching column name that corresponds to gender
                covid_search_terms - search terms for matching column name that corresponds to COVID status
                covid_status_search_terms - search terms for positive, negative, and suspected COVID status
        death_options: dict
            Options to pass to _patient_death; see _patient_death method for details
        gender_int_mappings: dict
            Options to define how to handle interger values for gender, e.g. {1: "female", 0: "male"}
        critical_care_options: dict
            Options to pass to _patient_critical_care_stay method call, see _patient_critical_care_stay method
            for details

        Returns
        -------
        dict
            Returns dictionary of basic results
        """
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
        """
        Checks that all patients in target files have been added to the database. Called prior to additional
        data entry beyond the Patients document/table and throws AssertionError if patients are missing.
        Error message prompts a call to add_patients.

        Parameters
        ----------
        patient_ids: list
            List of patients to check
        Returns
        -------
        None
        """
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
                     conflicts: str or None = None,
                     age_search_terms: list or None = None,
                     gender_search_terms: list or None = None,
                     covid_search_terms: list or None = None,
                     covid_status_search_terms: dict or None = None,
                     death_search_terms: list or None = None,
                     death_column: str = "destination",
                     death_file: str = "outcome",
                     gender_int_mappings: dict or None = None,
                     critical_care_file: str = "outcome",
                     critical_care_presence_infers_positivity: bool = False,
                     critical_care_column: str = "CRITICAL_CARE",
                     critical_care_pos_value: str = "Y") -> None:
        """
        Add all patients in target files, populating with basic information (age, gender, did they test COVID positive
        during their stay? Were they admitted to ICU during stay? Did the patient die during their stay?). This method
        MUST be called prior to all other methods, otherwise an AssertionError will be thrown.

        This method parses each target file and makes an entry for each unique patient. Basic information is acquired
        from parsing each target file. For age and gender a single unique value is expected that conforms across all
        entries for this patient. If a conflict is found it is handle according to the conflict settings (ignore or
        raise an error). For COVID 19 status, a summary of the patients status is made as follows:
            negative results only - negative status
            negative and positive results/suspected and positive results/positive results only - positive status
            negative and suspected results/suspected results only/no results - unknown status

        Events of critical care and death are found according to the appropriate parameters given and generate a boolean
        value signifying if this event occurred during stay.

        Parameters
        ----------
        conflicts: str, optional
            If given, overwrites the conflicts setting declared at object initiation
        age_search_terms: list, (default = ["^age$", "^age[.-_]+", "[.-_]+age"])
            Regular expression search terms to use when locating columns that correspond to age.
        gender_search_terms: list, (default = ["^gender$", "^gender[.-_]+", "[.-_]+gender", "^sex$", "^sex[.-_]+", "[.-_]+sex"])
            Regular expression search terms to use when locating columns that correspond to gender.
        covid_search_terms: list, (default = ["covid_status", "covid", "covid19"])
            Regular expression search terms to use when locating columns that correspond to gender.
        covid_status_search_terms: dict
            Search terms to use when classifying a patients COVID status based on the values extracted
            from target files
            Default = {"positive": ["+ve", "^p$", "^pos$", "^positive$"],
                       "negative": ["-ve", "^p$", "^neg$", "^negative$"],
                       "suspected": ["suspected"]}
        death_search_terms: list, (default = ["dead", "died", "death"])
            Regular expression search terms for determining if a patient died during stay
        death_column: list, (default = "destination")
            Name of column to search for values attributed to death during stay
        death_file: str, (default="outcome")
            Keyword to search for to acquire files that contain the above "death_column"
        gender_int_mappings: dict, (default = {0: "M", 1: "F"})
            If the located values for gender correspond to integer values, these mappings are used for assigning
            the resulting value
        critical_care_file: str, (default = "outcome")
            Name of the target file to find critical care events within
        critical_care_presence_infers_positivity: bool, (default=False)
            If True, the presence of a patient in the critical_care_file denotes a critical care stay event.
            If True, critical_care_column and critical_care_pos_value are ignored
        critical_care_column: str, (default = "CRITICAL_CARE")
            Name of the column to locate the critical care stay event in target file
        critical_care_pos_value: str, (default = "Y")
            Value that corresponds to a critical care stay occurring

        Returns
        -------
        None
        """
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
                            covid_status_search_terms=covid_status_search_terms)
        death_options = dict(death_column=death_column,
                             death_file=death_file,
                             search_terms=death_search_terms)
        critical_care_options = dict(critical_care_file=critical_care_file,
                                     critical_care_presence_infers_positivity=critical_care_presence_infers_positivity,
                                     critical_care_column=critical_care_column,
                                     critical_care_pos_value=critical_care_pos_value)

        if conflicts is not None:
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

    def _load_and_concat(self, filename:str):

        files = {name: properties for name, properties in self._files.items()
                 if filename.lower() in name.lower()}
        return pd.concat([self._load_dataframe(path=properties.get("path"),
                                               filetype=properties.get("type"))
                          for properties in files.values()], ignore_index=True)

    def add_outcome_events(self,
                           filename: str,
                           mappings: dict):
        """
        For each patient in the target files, add outcome events to database using target files that contain the
        keyword specified in filename

        Parameters
        ----------
        filename: str
            Keyword to use for capturing files that contain outcome events
        mappings: dict
            Column mappings, specific key values are required and should map to the relevant column within the
            target file(s). Expected keys: event_type, event_datetime, covid_status, death, critical_care_admission
            Additional optional keys: component, source_type, source, wimd

        Returns
        -------
        None
        """
        outcomes = self._load_and_concat(filename=filename)
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
                            if_exists="append")

    def add_measurements(self,
                         filename: str,
                         result_datetime: str or list):
        measurements = self._load_and_concat(filename=filename)
        self._assert_patients_added(measurements[self._id_column].values)
        if self._config.db_type == "nosql":
            pass
        # TODO if nosql - for each measurement assess the type and add_new_measurement
        # TODO if sql - for each measurement assess the type and add_new_measurement
        pass

    def add_critical_care_events(self):
        pass

    def add_comorbidities(self):
        pass

