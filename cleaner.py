import pandas as pd
import numpy as np

class DataCleaner:
    global disp
    
    """
    Parameters:
        data_file_path (str): The file path of the data you want to load
    """
    def __init__(self, data_file_path):
        self.data = pd.read_csv(data_file_path)

    def __read_file_source(self, filepath, source='excel', ):
        """
        Parameters:
            filepath (str): The location in the filesystem where the file is located
            source (str): The type of the file source ex. excel, parquet, json
        Returns:
            eval(expression): A function to be executed by pandas that reads a file
        """
        expression = 'pd.read_' + source.lower() + f'("{filepath}")'
        return eval(expression)

    def load_disposition_file(self, filepath, source='excel'):
        """
        Loads in the disposition file from an excel or csv file

        Parameters:
            filepath (str): The location in the filesystem where the file is located
            source (str): The type of the file source ex. excel, parquet, json
        Returns:
            disposition_data: The data about the disposition codes
        """
        self.disp = self.__read_file_source(filepath, source)
        return self.disp

    def __convert_disposition(self, dataframe):
        # Convert to dictionary
        converted_disposition = dict(zip(self.disp[self.disp.columns[0]], self.disp[self.disp.columns[1]]))
        return converted_disposition

    def convert_disposition_codes(self, value):
        """
        Uses the disposition codes and converts each key in a data frame 
        column to the proper value
        """
        chq = dict(zip(self.disp[self.disp.columns[0]], self.disp[self.disp.columns[1]]))
        return value if value not in chq else chq[value]

    def convert_q1_codes(self, value):
        """
        Uses the question codes and converts each key in a data frame 
        column to the proper value
        """
        question_codes = {
        '1':'Yes',
        '2':'No',
        '3':'Undecided',
        '4':'Refused',
        }
        # Remove spaces in string
        clean_string = value.replace(' ', '')
        # Assuming that the minimum someone enters is a number
        answer = clean_string[0]
        # If not, then return the word e.g if 'Yes' return 'Yes'
        return clean_string if answer not in question_codes else question_codes[answer]

    
    def clean_types(self):
        self.data['Congressional District'] = self.data['Congressional District'].fillna(1).astype(int)
        self.data['Call Date'] = pd.to_datetime(self.data['Call Date']).dt.strftime('%m/%d/%Y')
        self.data['Disposition'] = self.data['Disposition'].astype('str')
        self.data['Q1'] = self.data['Q1'].fillna(np.nan).replace([np.nan], [None]).astype('str')

    def clean_disposition_column(self):
        # Convert the data in dispositions column
        self.data['Disposition'] = self.data['Disposition'].apply(self.convert_disposition_codes)
        return self.data
    
    def clean_q1_column(self):
        # Convert the data in q1 column
        self.data['Q1'] = self.data['Q1'].apply(self.convert_q1_codes)
        return self.data

    def create_csv(self, filename):
        """ Creates a csv"""
        self.data.to_csv(filename)

    def create_report(self, column_name, titles):
        """
        Parameters: 
        column
            titles (str[]): The titles of the rows in the report
        Returns:
            report: A dataframe with daily counts and total

        """
        dates_in_order = pd.to_datetime(self.data['Call Date'].unique()).sort_values().strftime('%m/%d/%Y')

        # Error checking
        if column_name not in self.data.columns:
            raise Exception('Column name is not a valid column')
            
        # Create a report for disposition
        report = pd.DataFrame(columns=[x for x in dates_in_order], index=titles)
        # report.insert(0, "", None)
        report.insert(report.columns.size, "Total", None)

        # Check the number of rows in each column by date and answer for disposition
        result = []
        for idx, title in enumerate(titles):
            disposition_total = self.data[self.data[column_name] == title].shape[0]
            for date in dates_in_order:
                ctd = self.data[(self.data[column_name] == title) & (self.data['Call Date'] == date)].shape[0]
                result.append(ctd)
            result.append(disposition_total)
            report.iloc[idx, :] = result
            # Empty the array before looping again
            result = []
        return report

    def publish_report(self,filename, report1, report2):
        """Publishes the data as an excel file"""
        final_report = pd.concat([report1, report2])
        final_report.to_excel(filename)

if __name__ == '__main__':
    disposition_titles = ["Answered", "Do Not Call", "Left Message", "Bad Number", "Wrong Number"]
    q1_answers = ["Yes", "No", "Undecided", "Refused"]
    cleaner = DataCleaner('data/sample_data.csv')
    cleaner.load_disposition_file('data/disposition_definitions.xlsx')
    cleaner.clean_types()
    cleaner.clean_disposition_column()
    cleaner.clean_q1_column()
    cleaner.create_csv('clean_data_from_class.csv')
    rep1 = cleaner.create_report("Disposition", disposition_titles)
    rep2 = cleaner.create_report("Q1", q1_answers)
    cleaner.publish_report('rep_from_class.xlsx', rep1, rep2)
        
        

    

