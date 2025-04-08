import pandas

class DataIngestor:
    """
    Data processing engine for nutrition and obesity statistics.
    Handles complex data operations including filtering, aggregation,
    and statistical calculations.
    """
    
    def __init__(self, csv_path: str):
        """
        Initialize data processor with dataset path.
        
        Args:
            csv_path: Path to CSV file containing survey data
        """
        # Questions where lower values are better
        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]


        self.csv_path = csv_path
        self.data_set = self._load_dataset_with_validation()

    def _load_dataset_with_validation(self):
        """
        Load and validate dataset structure.
        
        Returns:
            pandas.DataFrame: Loaded dataset
            
        Raises:
            ValueError: If required columns are missing
        """
        dataset = pandas.read_csv(self.csv_path)
        required_columns = {'Question', 'YearStart', 'YearEnd', 'LocationDesc', 'Data_Value'}
        if not required_columns.issubset(dataset.columns):
            raise ValueError("CSV file missing required columns")
        return dataset

    def helper_for_states(self, question):
        """
        Execute full processing pipeline for all states.
        
        Args:
            question: Survey question to analyze
            
        Returns:
            dict: Processed results for all states
        """
        query_result = self.data_set[self.data_set['Question'] == question]
        return self._execute_state_processing_pipeline(query_result)

    def _execute_state_processing_pipeline(self, input_data):
        """
        Complete data processing workflow:
        1. Temporal filtering
        2. Numeric conversion
        3. Aggregation
        4. Sorting
        5. Formatting
        
        Args:
            input_data: Subset of data to process
            
        Returns:
            dict: Formatted results
        """
        filtered_by_time = self._apply_temporal_filters(input_data)
        numeric_converted = self._convert_to_numeric_values(filtered_by_time)
        aggregated_results = self._compute_state_aggregates(numeric_converted)
        sorted_output = self._sort_aggregated_results(aggregated_results)
        return self._format_final_output(sorted_output)

    def _apply_temporal_filters(self, data_subset):
        """
        Filter data to 2011-2022 timeframe.
        
        Args:
            data_subset: Data to filter
            
        Returns:
            pandas.DataFrame: Time-filtered data
        """
        return data_subset[
            (data_subset['YearStart'] >= 2011) & 
            (data_subset['YearEnd'] <= 2022)
        ]

    def _convert_to_numeric_values(self, data_subset):
        """
        Convert values to numeric type with error handling.
        
        Args:
            data_subset: Data to convert
            
        Returns:
            pandas.DataFrame: Cleaned numeric data
        """
        modified_data = data_subset.copy()
        modified_data['Data_Value'] = pandas.to_numeric(
            modified_data['Data_Value'],
            errors='coerce',  # Convert errors to NaN
            downcast='float'
        )
        return modified_data.dropna(subset=['Data_Value'])

    def _compute_state_aggregates(self, processed_data):
        """
        Calculate state-level averages.
        
        Args:
            processed_data: Cleaned numeric data
            
        Returns:
            pandas.Series: State averages
        """
        return processed_data.groupby('LocationDesc')['Data_Value'].mean()

    def _sort_aggregated_results(self, aggregated_data):
        """
        Sort states by calculated values.
        
        Args:
            aggregated_data: State averages
            
        Returns:
            pandas.Series: Sorted results
        """
        return aggregated_data.sort_values(ascending=True)

    def _format_final_output(self, sorted_data):
        """
        Convert processed results to API-friendly format.
        
        Args:
            sorted_data: Sorted state averages
            
        Returns:
            dict: Formatted output
        """
        return sorted_data.to_dict()

    def helper_for_state(self, data):
        """
        Execute processing pipeline for single state.
        
        Args:
            data: Pre-filtered state data
            
        Returns:
            dict: Processed state results
        """
        temporal_filtered = self._apply_temporal_filters(data)
        numeric_converted = self._convert_to_numeric_values(temporal_filtered)
        aggregated = self._compute_state_aggregates(numeric_converted)
        sorted_results = self._sort_aggregated_results(aggregated)
        return self._format_final_output(sorted_results)

    def states_mean(self, data):
        """
        Calculate state averages for a specific question.
        
        Args:
            data: Contains 'question' parameter
            
        Returns:
            dict: State averages or empty dict if invalid input
        """
        question_text = data.get('question', '')
        if not question_text:
            return {}
        return self.helper_for_states(question_text)

    def state_mean(self, data):
        """
        Calculate average for specific state and question.
        
        Args:
            data: Contains 'question' and 'state' parameters
            
        Returns:
            dict: State average or empty dict if invalid input
        """
        question_param = data.get('question', '')
        state_param = data.get('state', '')
        
        if not question_param or not state_param:
            return {}

        base_data = self.data_set[self.data_set['Question'] == question_param]
        if state_param not in base_data['LocationDesc'].unique():
            return {}

        state_specific_data = base_data[base_data['LocationDesc'] == state_param]
        return self._execute_state_processing_pipeline(state_specific_data)

    def best5(self, data):
        """
        Identify top 5 states based on question type.
        
        Args:
            data: Contains 'question' parameter
            
        Returns:
            dict: Top 5 states based on metric direction
        """
        question_identifier = data.get('question', '')
        if not question_identifier:
            return {}

        complete_results = self.states_mean(data)
        items_list = list(complete_results.items())
        
        # Handle different metric directions
        if question_identifier in self.questions_best_is_min:
            return dict(items_list[:5])  # Lowest values first
        elif question_identifier in self.questions_best_is_max:
            reversed_list = list(reversed(items_list))
            return dict(reversed_list[:5])  # Highest values first
        return {}

    def worst5(self, data):
        """
        Identify bottom 5 states based on question type.
        
        Args:
            data: Contains 'question' parameter
            
        Returns:
            dict: Bottom 5 states based on metric direction
        """
        question_identifier = data.get('question', '')
        if not question_identifier:
            return {}

        complete_results = self.states_mean(data)
        items_list = list(complete_results.items())
        
        # Inverse logic of best5
        if question_identifier in self.questions_best_is_max:
            return dict(items_list[:5])
        elif question_identifier in self.questions_best_is_min:
            reversed_list = list(reversed(items_list))
            return dict(reversed_list[:5])
        return {}

    def global_mean(self, data):
        """
        Calculate global average for a question.
        
        Args:
            data: Contains 'question' parameter
            
        Returns:
            dict: Global average with error handling
        """
        target_question = data.get('question', None)
        
        if not target_question or target_question.strip() == '':
            return {'global_mean': 0.0}
        
        working_dataset = self.data_set.copy()
        question_filter_mask = working_dataset['Question'] == target_question
        filtered_by_question_data = working_dataset.loc[question_filter_mask]
        
        converted_values_series = pandas.to_numeric(
            filtered_by_question_data['Data_Value'], 
            errors='coerce',
            downcast='float'
        )
        
        cleaned_values = converted_values_series.dropna()
        
        if cleaned_values.empty:
            average_value = 0.0
        else:
            try:
                average_value = cleaned_values.astype('float64').mean()
            except Exception as e:
                average_value = 0.0
        
        return {'global_mean': average_value}

    def diff_from_mean(self, input_data):
        """
        Calculate deviations from global mean for all states.
        
        Args:
            input_data: Contains 'question' parameter
            
        Returns:
            dict: Deviation values for all states
        """
        query_data = {'question': input_data['question']}
        all_states = self.data_set['LocationDesc'].unique()
        global_avg = self.global_mean(input_data)

        differences = {}
        for region in all_states:
            query_data['state'] = region
            state_avg = self.state_mean(query_data)
            if state_avg:
                deviation = global_avg['global_mean'] - state_avg[region]
                differences[region] = deviation
            query_data.pop('state', None)

        return differences

    def state_diff_from_mean(self, data):
        """
        Calculate single state's deviation from global mean.
        
        Args:
            data: Contains 'question' and 'state' parameters
            
        Returns:
            dict: Deviation value for specified state
        """
        question_identifier = data.get('question', '')
        state_identifier = data.get('state', '')
        
        if not question_identifier or not state_identifier:
            return {}

        national_avg = self.global_mean({'question': question_identifier})['global_mean']
        state_avg = self.state_mean(data).get(state_identifier, 0)
        return {state_identifier: national_avg - state_avg}

    def state_mean_by_category(self, data):
        """
        Calculate stratified averages for a state.
        
        Args:
            data: Contains 'question' and 'state' parameters
            
        Returns:
            dict: Category-based averages for specified state
        """
        question_identifier = data.get('question', '')
        state_identifier = data.get('state', '')
        
        if not question_identifier or not state_identifier:
            return {}

        base_data = self.data_set[self.data_set['Question'] == question_identifier]
        state_data = base_data[base_data['LocationDesc'] == state_identifier]
        
        if state_data.empty:
            return {}

        numeric_converted = self._convert_to_numeric_values(state_data)
        stratified_means = numeric_converted.groupby(
            ['StratificationCategory1', 'Stratification1']
        )['Data_Value'].mean()
        
        return {state_identifier: {str(k): v for k, v in stratified_means.items()}}

    def mean_by_category(self, data):
        """
        Calculate multi-dimensional category averages.
        
        Args:
            data: Contains 'question' parameter
            
        Returns:
            dict: Complex stratified averages across all states
        """
        question_identifier = data.get('question', '')
        if not question_identifier:
            return {}

        filtered_data = self.data_set[self.data_set['Question'] == question_identifier]
        numeric_converted = self._convert_to_numeric_values(filtered_data)
        stratified_means = numeric_converted.groupby(
            ['LocationDesc', 'StratificationCategory1', 'Stratification1']
        )['Data_Value'].mean()
        
        return {str(k): v for k, v in stratified_means.items()}